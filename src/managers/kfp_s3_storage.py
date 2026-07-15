#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for KFP S3 storage / object storage related tasks."""

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from constants import DEFAULT_PIPELINE_ROOT_TEMPLATE, KFP_S3_STORAGE, S3_REQUIRED_FIELDS
from core.config import KFPS3StorageConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.manifests import KubernetesManifestsManager
from utils.helpers_manifests import (
    generate_artifact_repositories_configmap_manifest,
    generate_kfp_launcher_configmap_manifest,
    generate_minio_artifact_secret_manifest,
)
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


class KFPS3StorageManager(ManagerStatusProtocol, WithLogging):
    """Manager for the kfp-s3-storage relation.

    Generates the Kubeflow multi-tenancy artifact-store resources (the
    ``mlpipeline-minio-artifact`` Secret and the ``artifact-repositories`` and
    ``kfp-launcher`` ConfigMaps) from the credentials advertised by an S3 provider.
    """

    def __init__(self, state: GlobalState):
        self.name = KFP_S3_STORAGE
        self.state = state
        self.manifest_manager = KubernetesManifestsManager(state)

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component.

        KFP S3 storage integration is optional and purely relation-driven. Once an S3 provider
        has advertised some credentials, the charm blocks if any of the mandatory fields
        (access-key, secret-key, bucket, endpoint) are still missing, or if the
        ``kfp-pipeline-root`` config option uses an unsupported scheme (e.g. ``s3://``).
        """
        status_list: list[StatusObject] = []

        try:
            KFPS3StorageConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.warning(str(err))
            invalid = [
                str(error["loc"][0]) for error in err.errors() if error["type"] != "missing"
            ]
            if invalid:
                status_list.append(ConfigStatuses.invalid_config_parameters(fields=invalid))

        connection_info = self.state.kfp_s3_connection_info
        if connection_info:
            missing = [field for field in S3_REQUIRED_FIELDS if not connection_info.get(field)]
            if missing:
                status_list.append(CharmStatuses.missing_kfp_s3_credentials(fields=missing))

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifests for the current kfp-s3-storage relation."""
        if not (self.state.is_kfp_s3_related() and self.state.profile_config):
            return ReconciledManifests()

        connection_info = self.state.kfp_s3_connection_info
        missing = [field for field in S3_REQUIRED_FIELDS if not connection_info.get(field)]
        if missing:
            self.logger.warning(
                f"KFP S3 storage connection info is incomplete (missing {', '.join(missing)}), "
                "skipping manifests generation"
            )
            return ReconciledManifests()

        access_key = connection_info["access-key"]
        secret_key = connection_info["secret-key"]
        bucket = connection_info["bucket"]
        endpoint = connection_info["endpoint"]
        region = connection_info.get("region")

        profile = self.state.profile_config.profile

        secrets_manifests = (
            [
                generate_minio_artifact_secret_manifest(
                    self.manifest_manager.minio_artifact_secret_template,
                    profile,
                    access_key,
                    secret_key,
                )
            ]
            if self.state.is_k8s_secrets_manifests_related()
            else []
        )

        configmaps_manifests = []
        if self.state.is_k8s_configmaps_manifests_related():
            kfp_s3_config = self.state.kfp_s3_config
            default_pipeline_root = (
                kfp_s3_config.default_pipeline_root
                if kfp_s3_config and kfp_s3_config.default_pipeline_root
                else DEFAULT_PIPELINE_ROOT_TEMPLATE.format(bucket=bucket)
            )
            configmaps_manifests = [
                generate_artifact_repositories_configmap_manifest(
                    self.manifest_manager.artifact_repositories_template,
                    profile,
                    bucket,
                    endpoint,
                ),
                generate_kfp_launcher_configmap_manifest(
                    self.manifest_manager.kfp_launcher_template,
                    profile,
                    endpoint,
                    region,
                    default_pipeline_root,
                ),
            ]

        return ReconciledManifests(secrets=secrets_manifests, configmaps=configmaps_manifests)
