#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import base64
import json
import logging
from pathlib import Path

import jubilant
import lightkube
import yaml
from helpers import get_application_data
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
S3_INTEGRATOR_CHANNEL = "1/stable"

S3_CREDENTIALS_RELATION_NAME = "s3-credentials"
SECRETS_RELATION_NAME = "secrets"
CONFIG_MAPS_RELATION_NAME = "config-maps"

MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio-secret-key"
MINIO_BUCKET = "mlpipeline"
MINIO_ENDPOINT = "http://minio.kubeflow.svc.cluster.local:9000"
MINIO_ENDPOINT_HOST = "minio.kubeflow.svc.cluster.local:9000"
MINIO_REGION = "us-east-1"

EXPECTED_MINIO_SECRET_NAME = "mlpipeline-minio-artifact"
EXPECTED_ARTIFACT_REPOSITORIES_CONFIGMAP = "artifact-repositories"
EXPECTED_KFP_LAUNCHER_CONFIGMAP = "kfp-launcher"
ARTIFACT_REPOSITORY_REF = "default-namespaced"


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


def test_deploy_and_configure_kf_integrator(juju: jubilant.Juju, kubeflow_integrator: str):
    """Deploy the kubeflow integrator charm and the resource-dispatcher stack."""
    logger.info("Deploying Kubeflow Integrator charm...")
    juju.deploy(
        kubeflow_integrator,
        app=KUBEFLOW_INTEGRATOR,
        config={"profile": KUBEFLOW_USER_PROFILE_A_NAME},
    )

    logger.info("Deploying resource-dispatcher and its dependencies...")
    juju.deploy(METACONTROLLER_CHARM, channel=METACONTROLLER_CHARM_CHANNEL, trust=True)
    juju.deploy(ADMISSION_WEBHOOK, channel=ADMISSION_WEBHOOK_CHANNEL, trust=True)
    juju.deploy(RESOURCE_DISPATCHER, channel=RESOURCE_DISPATCHER_CHANNEL, trust=True)

    # S3 integration is optional and relation-driven, so the integrator settles as active on its
    # own before any S3 provider is related.
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_integrate_with_s3_integrator(juju: jubilant.Juju):
    """Deploy s3-integrator, advertise MinIO credentials and integrate with the integrator."""
    logger.info("Deploying s3-integrator charm...")
    juju.deploy(
        S3_INTEGRATOR,
        app=S3_INTEGRATOR,
        channel=S3_INTEGRATOR_CHANNEL,
        config={
            "bucket": MINIO_BUCKET,
            "endpoint": MINIO_ENDPOINT,
            "region": MINIO_REGION,
        },
    )

    logger.info("Integrating kubeflow-integrator with s3-integrator...")
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{S3_CREDENTIALS_RELATION_NAME}",
        f"{S3_INTEGRATOR}:{S3_CREDENTIALS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_s3_manifests_generated_in_relation_data(juju: jubilant.Juju):
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
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
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
        == base64.b64encode(MINIO_ACCESS_KEY.encode()).decode()
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
                MINIO_ACCESS_KEY.encode()
            ).decode("utf-8")
            assert minio_secret.data["secretkey"] == base64.b64encode(
                MINIO_SECRET_KEY.encode()
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
            assert repository["s3"]["endpoint"] == MINIO_ENDPOINT_HOST

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
            assert providers["s3"]["default"]["endpoint"] == MINIO_ENDPOINT_HOST
            assert providers["s3"]["default"]["region"] == MINIO_REGION


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
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR),
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
