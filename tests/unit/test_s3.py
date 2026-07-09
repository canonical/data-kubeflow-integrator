#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit test module for data-kubeflow-integrator integration with S3 / object storage."""

import base64
import dataclasses
from pathlib import Path

import yaml
from charms.data_platform_libs.v0.data_models import json
from ops import ActiveStatus, BlockedStatus, testing
from ops.testing import Relation, State

from charm import KubeflowIntegratorCharm

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())

S3_CREDENTIALS = {
    "access-key": "minio-access-key",
    "secret-key": "minio-secret-key",
    "bucket": "mlpipeline",
    "endpoint": "http://minio.kubeflow:9000",
    "region": "us-east-1",
}


def _get_manifests(state_out: State, relation: Relation) -> list[dict]:
    """Extract the list of kubernetes manifests shared over a resource-dispatcher relation."""
    app_data = state_out.get_relation(relation.id).local_app_data
    assert app_data["is_secret"] == "true"
    manifests_secret_id = app_data["kubernetes_manifests"]
    manifest_secret = [secret for secret in state_out.secrets if secret.id == manifests_secret_id][
        0
    ]
    secret_content = manifest_secret.latest_content
    assert secret_content is not None
    return json.loads(secret_content["manifests"])


def test_charm_active_when_s3_pipeline_root_set_without_relation(
    charm_configuration: dict, base_state: State
):
    """Check that setting 's3-default-pipeline-root' alone does not block the charm.

    S3 integration is purely relation-driven, so the config option is optional and must not
    put the charm in a blocked status when no S3 provider is related.
    """
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    charm_configuration["options"]["s3-default-pipeline-root"]["default"] = (
        "minio://mlpipeline/custom"
    )
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    state_out = ctx.run(ctx.on.start(), base_state)
    assert state_out.app_status == ActiveStatus()


def test_s3_manifests_generated_when_related(charm_configuration: dict, base_state: State):
    """Check that the artifact-store Secret and ConfigMaps are generated for an S3 relation."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_relation = Relation(
        endpoint="s3-credentials", interface="s3", remote_app_data=dict(S3_CREDENTIALS)
    )
    secrets_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, secrets_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(s3_relation), state_in)
    assert state_out.app_status == ActiveStatus()

    # The mlpipeline-minio-artifact Secret is shared over the secrets relation.
    secret_manifests = _get_manifests(state_out, secrets_relation)
    assert len(secret_manifests) == 1
    minio_secret = secret_manifests[0]
    assert minio_secret["apiVersion"] == "v1"
    assert minio_secret["kind"] == "Secret"
    assert minio_secret["metadata"]["name"] == "mlpipeline-minio-artifact"
    assert minio_secret["metadata"]["namespace"] == "profile-name"
    assert minio_secret["data"]["accesskey"] == base64.b64encode(b"minio-access-key").decode()
    assert minio_secret["data"]["secretkey"] == base64.b64encode(b"minio-secret-key").decode()

    # The artifact-repositories and kfp-launcher ConfigMaps are shared over the config-maps relation.
    config_map_manifests = _get_manifests(state_out, config_maps_relation)
    assert len(config_map_manifests) == 2
    config_maps = {cm["metadata"]["name"]: cm for cm in config_map_manifests}

    artifact_repositories = config_maps["artifact-repositories"]
    assert artifact_repositories["kind"] == "ConfigMap"
    assert artifact_repositories["metadata"]["namespace"] == "profile-name"
    assert (
        artifact_repositories["metadata"]["annotations"][
            "workflows.argoproj.io/default-artifact-repository"
        ]
        == "default-namespaced"
    )
    repository = yaml.safe_load(artifact_repositories["data"]["default-namespaced"])
    assert repository["s3"]["bucket"] == "mlpipeline"
    assert repository["s3"]["endpoint"] == "minio.kubeflow:9000"
    assert repository["s3"]["insecure"] is True
    assert repository["s3"]["accessKeySecret"] == {
        "name": "mlpipeline-minio-artifact",
        "key": "accesskey",
    }
    assert repository["s3"]["secretKeySecret"] == {
        "name": "mlpipeline-minio-artifact",
        "key": "secretkey",
    }

    kfp_launcher = config_maps["kfp-launcher"]
    assert kfp_launcher["kind"] == "ConfigMap"
    assert kfp_launcher["metadata"]["namespace"] == "profile-name"
    # Falls back to the templated default pipeline root when the config is unset.
    assert kfp_launcher["data"]["defaultPipelineRoot"] == "minio://mlpipeline/v2/artifacts"
    providers = yaml.safe_load(kfp_launcher["data"]["providers"])
    assert providers["s3"]["default"]["endpoint"] == "minio.kubeflow:9000"
    assert providers["s3"]["default"]["disableSSL"] is True
    assert providers["s3"]["default"]["region"] == "us-east-1"
    assert providers["s3"]["default"]["credentials"]["secretRef"]["secretName"] == (
        "mlpipeline-minio-artifact"
    )


def test_s3_endpoint_without_scheme_infers_tls_from_port(
    charm_configuration: dict, base_state: State
):
    """Check that a scheme-less endpoint infers TLS from its port (443 -> secure).

    The endpoint is parsed with urlparse, so a bare ``host:443`` must be treated as secure
    (``insecure``/``disableSSL`` false) while preserving the ``host:port`` form.
    """
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_credentials = {**S3_CREDENTIALS, "endpoint": "minio.kubeflow:443"}
    s3_relation = Relation(
        endpoint="s3-credentials", interface="s3", remote_app_data=s3_credentials
    )
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(s3_relation), state_in)

    config_map_manifests = _get_manifests(state_out, config_maps_relation)
    config_maps = {cm["metadata"]["name"]: cm for cm in config_map_manifests}

    artifact_repositories = config_maps["artifact-repositories"]
    repository = yaml.safe_load(artifact_repositories["data"]["default-namespaced"])
    assert repository["s3"]["endpoint"] == "minio.kubeflow:443"
    assert repository["s3"]["insecure"] is False

    kfp_launcher = config_maps["kfp-launcher"]
    providers = yaml.safe_load(kfp_launcher["data"]["providers"])
    assert providers["s3"]["default"]["endpoint"] == "minio.kubeflow:443"
    assert providers["s3"]["default"]["disableSSL"] is False


def test_s3_default_pipeline_root_config_overrides_template(
    charm_configuration: dict, base_state: State
):
    """Check that the 's3-default-pipeline-root' config overrides the kfp-launcher default."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    charm_configuration["options"]["s3-default-pipeline-root"]["default"] = (
        "minio://mlpipeline/custom-root"
    )
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_relation = Relation(
        endpoint="s3-credentials", interface="s3", remote_app_data=dict(S3_CREDENTIALS)
    )
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(s3_relation), state_in)

    config_map_manifests = _get_manifests(state_out, config_maps_relation)
    kfp_launcher = [cm for cm in config_map_manifests if cm["metadata"]["name"] == "kfp-launcher"][
        0
    ]
    assert kfp_launcher["data"]["defaultPipelineRoot"] == "minio://mlpipeline/custom-root"


def test_s3_manifests_omit_namespace_for_wildcard_profile(
    charm_configuration: dict, base_state: State
):
    """Check that the artifact-store resources omit the namespace for the wildcard profile."""
    charm_configuration["options"]["profile"]["default"] = "*"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_relation = Relation(
        endpoint="s3-credentials", interface="s3", remote_app_data=dict(S3_CREDENTIALS)
    )
    secrets_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, secrets_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(s3_relation), state_in)

    minio_secret = _get_manifests(state_out, secrets_relation)[0]
    assert "namespace" not in minio_secret["metadata"]

    for config_map in _get_manifests(state_out, config_maps_relation):
        assert "namespace" not in config_map["metadata"]


def test_no_s3_manifests_when_credentials_incomplete(charm_configuration: dict, base_state: State):
    """Check that incomplete S3 credentials block the charm and generate no manifests."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    # Missing the mandatory secret-key: the S3 lib does not emit credentials_changed and the
    # manifests must not be generated.
    s3_relation = Relation(
        endpoint="s3-credentials",
        interface="s3",
        remote_app_data={"access-key": "minio-access-key", "bucket": "mlpipeline"},
    )
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(config_maps_relation), state_in)

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing S3 field(s): 'secret-key', 'endpoint'" in status.message
    config_map_manifests = _get_manifests(state_out, config_maps_relation)
    assert config_map_manifests == []


def test_no_s3_manifests_when_bucket_and_endpoint_missing(
    charm_configuration: dict, base_state: State
):
    """Check that credentials lacking a bucket and endpoint block the charm and skip manifests.

    The mandatory access-key/secret-key are present (so the relation is considered ready), but
    without a bucket and endpoint the manifests cannot be rendered and the charm blocks.
    """
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_relation = Relation(
        endpoint="s3-credentials",
        interface="s3",
        remote_app_data={"access-key": "minio-access-key", "secret-key": "minio-secret-key"},
    )
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(s3_relation), state_in)

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing S3 field(s): 'bucket', 'endpoint'" in status.message
    assert _get_manifests(state_out, config_maps_relation) == []


def test_s3_secret_generated_without_config_maps_relation(
    charm_configuration: dict, base_state: State
):
    """Check that only the Secret is generated when the config-maps relation is absent.

    The artifact-store Secret is gated on the secrets relation while the ConfigMaps are gated on
    the config-maps relation, so each resource must be shared independently of the other.
    """
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_relation = Relation(
        endpoint="s3-credentials", interface="s3", remote_app_data=dict(S3_CREDENTIALS)
    )
    secrets_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, secrets_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(s3_relation), state_in)

    assert state_out.app_status == ActiveStatus()
    secret_manifests = _get_manifests(state_out, secrets_relation)
    assert len(secret_manifests) == 1
    assert secret_manifests[0]["metadata"]["name"] == "mlpipeline-minio-artifact"


def test_s3_manifests_cleared_when_credentials_gone(charm_configuration: dict, base_state: State):
    """Check that the artifact-store manifests are cleared when the S3 relation is broken."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    s3_relation = Relation(
        endpoint="s3-credentials", interface="s3", remote_app_data=dict(S3_CREDENTIALS)
    )
    secrets_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    config_maps_relation = Relation(endpoint="config-maps", interface="kubernetes_manifest")
    relations = [*base_state.relations, s3_relation, secrets_relation, config_maps_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_broken(s3_relation), state_in)

    assert state_out.app_status == ActiveStatus()
    assert _get_manifests(state_out, secrets_relation) == []
    assert _get_manifests(state_out, config_maps_relation) == []
