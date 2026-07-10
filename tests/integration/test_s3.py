#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import base64
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

import jubilant
import lightkube
import pytest
import yaml
from helpers import get_application_data
from helpers_s3 import S3ConnectionInfo, setup_microceph
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import ConfigMap, Secret
from tenacity import Retrying, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

KUBEFLOW_INTEGRATOR = "kubeflow-integrator"
KUBEFLOW_USER_PROFILE_A_NAME = "kubeflow-profile-a"

METACONTROLLER_CHARM = "metacontroller-operator"
METACONTROLLER_CHARM_CHANNEL = "latest/edge"
RESOURCE_DISPATCHER = "resource-dispatcher"
RESOURCE_DISPATCHER_CHANNEL = "latest/edge"
ADMISSION_WEBHOOK = "admission-webhook"
ADMISSION_WEBHOOK_CHANNEL = "latest/edge"

S3_INTEGRATOR = "s3-integrator"
S3_INTEGRATOR_CHANNEL = "2/edge"
S3_CREDENTIALS_SECRET = "s3-integrator-credentials"

S3_CREDENTIALS_RELATION_NAME = "kfp-s3-storage"
SECRETS_RELATION_NAME = "secrets"
CONFIG_MAPS_RELATION_NAME = "config-maps"

MINIO_BUCKET = "mlpipeline"

EXPECTED_MINIO_SECRET_NAME = "mlpipeline-minio-artifact"
EXPECTED_ARTIFACT_REPOSITORIES_CONFIGMAP = "artifact-repositories"
EXPECTED_KFP_LAUNCHER_CONFIGMAP = "kfp-launcher"
ARTIFACT_REPOSITORY_REF = "default-namespaced"


@pytest.fixture(scope="module")
def s3_connection_info() -> S3ConnectionInfo:
    """Provision a real, reachable S3 backend and return its connection info.

    ``s3-integrator`` validates its configuration by connecting to the endpoint and
    ensuring the bucket exists before it shares anything over the relation, so the tests
    need a real backend rather than a placeholder endpoint. ``setup_microceph`` provisions
    one with microceph, or reuses an external S3 when the ``S3_*`` env vars are set.
    """
    return setup_microceph()


def _endpoint_host(endpoint: str) -> str:
    """Return the ``host[:port]`` portion of an S3 endpoint, stripping any scheme."""
    return urlparse(endpoint if "://" in endpoint else f"//{endpoint}").netloc


def _get_manifests_from_relation(juju: jubilant.Juju, relation_name: str) -> list[dict]:
    """Return the list of manifests shared with resource-dispatcher over a relation."""
    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=relation_name
            ).values()
        )
    )
    secret_uri = relation_data["kubernetes_manifests"]
    secret_content = juju.show_secret(identifier=secret_uri, reveal=True).content
    return json.loads(secret_content["manifests"])


def test_deploy_and_configure_kf_integrator(
    juju: jubilant.Juju, kubeflow_integrator: str, kubeflow_user_profile_a: str
):
    """Deploy the kubeflow integrator charm and the resource-dispatcher stack."""
    # Create the ``kubeflow_user_profile_a`` profile first
    logger.info("Deploying Kubeflow Integrator charm...")
    juju.deploy(
        kubeflow_integrator,
        app=KUBEFLOW_INTEGRATOR,
        config={"profile": kubeflow_user_profile_a},
    )

    logger.info("Deploying resource-dispatcher and its dependencies...")
    juju.deploy(METACONTROLLER_CHARM, channel=METACONTROLLER_CHARM_CHANNEL, trust=True)
    juju.deploy(ADMISSION_WEBHOOK, channel=ADMISSION_WEBHOOK_CHANNEL, trust=True)
    juju.deploy(RESOURCE_DISPATCHER, channel=RESOURCE_DISPATCHER_CHANNEL, trust=True)

    # S3 integration is optional, so expect Active status
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_integrate_with_s3_integrator(juju: jubilant.Juju, s3_connection_info: S3ConnectionInfo):
    """Deploy s3-integrator and integrate with the integrator."""
    logger.info("Deploying s3-integrator charm...")
    config = {
        "bucket": MINIO_BUCKET,
        "endpoint": s3_connection_info.endpoint,
        "region": s3_connection_info.region,
    }
    # A TLS-enabled endpoint (e.g. microceph's RADOS Gateway) needs its CA chain so that
    # s3-integrator can reach the endpoint to ensure the bucket exists.
    if s3_connection_info.tls_ca_chain:
        config["tls-ca-chain"] = s3_connection_info.tls_ca_chain
    juju.deploy(
        S3_INTEGRATOR,
        app=S3_INTEGRATOR,
        channel=S3_INTEGRATOR_CHANNEL,
        config=config,
    )

    logger.info("Providing S3 credentials to s3-integrator via a user secret...")
    secret_uri = juju.add_secret(
        S3_CREDENTIALS_SECRET,
        {
            "access-key": s3_connection_info.access_key,
            "secret-key": s3_connection_info.secret_key,
        },
    )
    juju.grant_secret(secret_uri, S3_INTEGRATOR)
    juju.config(S3_INTEGRATOR, {"credentials": secret_uri})

    logger.info("Integrating kubeflow-integrator with s3-integrator...")
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{S3_CREDENTIALS_RELATION_NAME}",
        f"{S3_INTEGRATOR}:{S3_CREDENTIALS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status),
        delay=5,
    )


def test_s3_manifests_generated_in_relation_data(
    juju: jubilant.Juju, s3_connection_info: S3ConnectionInfo
):
    """Integrate over secrets/config-maps and check the artifact-store manifests are shared."""
    logger.info("Integrating kubeflow-integrator <> resource-dispatcher over secrets...")
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{SECRETS_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{SECRETS_RELATION_NAME}",
    )
    logger.info("Integrating kubeflow-integrator <> resource-dispatcher over config-maps...")
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{CONFIG_MAPS_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{CONFIG_MAPS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status),
        delay=5,
    )

    secret_manifests = _get_manifests_from_relation(juju, SECRETS_RELATION_NAME)
    minio_secret = [
        res
        for res in secret_manifests
        if res["kind"] == "Secret" and res["metadata"]["name"] == EXPECTED_MINIO_SECRET_NAME
    ]
    assert len(minio_secret) == 1
    assert minio_secret[0]["metadata"]["namespace"] == KUBEFLOW_USER_PROFILE_A_NAME
    assert (
        minio_secret[0]["data"]["accesskey"]
        == base64.b64encode(s3_connection_info.access_key.encode()).decode()
    )

    config_map_manifests = _get_manifests_from_relation(juju, CONFIG_MAPS_RELATION_NAME)
    config_map_names = {
        res["metadata"]["name"] for res in config_map_manifests if res["kind"] == "ConfigMap"
    }
    assert EXPECTED_ARTIFACT_REPOSITORIES_CONFIGMAP in config_map_names
    assert EXPECTED_KFP_LAUNCHER_CONFIGMAP in config_map_names


def test_s3_resources_created_in_profile_namespace(
    juju: jubilant.Juju,
    lightkube_client: lightkube.Client,
    kubeflow_user_profile_a: str,
    s3_connection_info: S3ConnectionInfo,
):
    """Check that resource-dispatcher applies the artifact-store resources to the profile namespace."""
    logger.info("Checking the mlpipeline-minio-artifact Secret is created...")
    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        with attempt:
            minio_secret = lightkube_client.get(
                Secret, name=EXPECTED_MINIO_SECRET_NAME, namespace=kubeflow_user_profile_a
            )
            assert minio_secret.data is not None
            assert minio_secret.data["accesskey"] == base64.b64encode(
                s3_connection_info.access_key.encode()
            ).decode("utf-8")
            assert minio_secret.data["secretkey"] == base64.b64encode(
                s3_connection_info.secret_key.encode()
            ).decode("utf-8")

    logger.info("Checking the artifact-repositories ConfigMap is created...")
    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        with attempt:
            artifact_repositories = lightkube_client.get(
                ConfigMap,
                name=EXPECTED_ARTIFACT_REPOSITORIES_CONFIGMAP,
                namespace=kubeflow_user_profile_a,
            )
            assert artifact_repositories.metadata is not None
            assert artifact_repositories.metadata.annotations is not None
            assert (
                artifact_repositories.metadata.annotations[
                    "workflows.argoproj.io/default-artifact-repository"
                ]
                == ARTIFACT_REPOSITORY_REF
            )
            assert artifact_repositories.data is not None
            repository = yaml.safe_load(artifact_repositories.data[ARTIFACT_REPOSITORY_REF])
            assert repository["s3"]["bucket"] == MINIO_BUCKET
            assert repository["s3"]["endpoint"] == _endpoint_host(s3_connection_info.endpoint)

    logger.info("Checking the kfp-launcher ConfigMap is created...")
    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        with attempt:
            kfp_launcher = lightkube_client.get(
                ConfigMap,
                name=EXPECTED_KFP_LAUNCHER_CONFIGMAP,
                namespace=kubeflow_user_profile_a,
            )
            assert kfp_launcher.data is not None
            assert "defaultPipelineRoot" in kfp_launcher.data
            providers = yaml.safe_load(kfp_launcher.data["providers"])
            assert providers["s3"]["default"]["endpoint"] == _endpoint_host(
                s3_connection_info.endpoint
            )
            assert providers["s3"]["default"]["region"] == s3_connection_info.region


def test_s3_charm_blocks_on_missing_fields(
    juju: jubilant.Juju, s3_connection_info: S3ConnectionInfo
):
    """Test that kubeflow-integrator blocks when the S3 provider advertises incomplete fields."""
    logger.info("Unsetting bucket and endpoint on s3-integrator to force partial data...")
    juju.cli("config", S3_INTEGRATOR, "--reset", "bucket,endpoint")

    logger.info("Waiting for kubeflow-integrator to block on missing S3 fields...")
    juju.wait(
        lambda status: (
            jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR)
            and status.apps[KUBEFLOW_INTEGRATOR].app_status.current == "blocked"
        ),
        delay=5,
    )
    message = juju.status().apps[KUBEFLOW_INTEGRATOR].app_status.message
    assert "Missing KFP field(s)" in message
    assert "'bucket'" in message
    assert "'endpoint'" in message

    logger.info("Restoring valid bucket and endpoint on s3-integrator...")
    juju.config(
        S3_INTEGRATOR,
        {"bucket": MINIO_BUCKET, "endpoint": s3_connection_info.endpoint},
    )
    juju.wait(
        lambda status: (
            jubilant.all_active(status, KUBEFLOW_INTEGRATOR)
            and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR)
        ),
        delay=5,
    )


def test_remove_s3_integration(
    juju: jubilant.Juju,
    lightkube_client: lightkube.Client,
    kubeflow_user_profile_a: str,
):
    """Remove the S3 integration and check that the artifact-store resources are deleted."""
    logger.info("Removing integration between kubeflow-integrator and s3-integrator...")
    juju.remove_relation(
        f"{KUBEFLOW_INTEGRATOR}:{S3_CREDENTIALS_RELATION_NAME}",
        f"{S3_INTEGRATOR}:{S3_CREDENTIALS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: (
            jubilant.all_active(status, KUBEFLOW_INTEGRATOR)
            and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR)
        ),
        delay=5,
    )

    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        for resource_type, resource_name in [
            (Secret, EXPECTED_MINIO_SECRET_NAME),
            (ConfigMap, EXPECTED_ARTIFACT_REPOSITORIES_CONFIGMAP),
            (ConfigMap, EXPECTED_KFP_LAUNCHER_CONFIGMAP),
        ]:
            with attempt:
                try:
                    lightkube_client.get(
                        resource_type, name=resource_name, namespace=kubeflow_user_profile_a
                    )
                    raise AssertionError(
                        f"Expected {resource_type} {resource_name} to be deleted, but it still exists."
                    )
                except ApiError:
                    pass  # this is exactly what's expected
