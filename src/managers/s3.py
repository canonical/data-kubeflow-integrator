#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for S3 / object storage related tasks."""

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope

from constants import DEFAULT_PIPELINE_ROOT_TEMPLATE, S3
from core.state import GlobalState
from core.statuses import CharmStatuses
from utils.helpers_manifests import (
    generate_artifact_repositories_configmap_manifest,
    generate_kfp_launcher_configmap_manifest,
    generate_minio_artifact_secret_manifest,
)
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


class S3Manager(ManagerStatusProtocol, WithLogging):
    """Manager for the s3-credentials relation.

    Generates the Kubeflow multi-tenancy artifact-store resources (the
    ``mlpipeline-minio-artifact`` Secret and the ``artifact-repositories`` and
    ``kfp-launcher`` ConfigMaps) from the credentials advertised by an S3 provider.
    """

    def __init__(self, state: GlobalState):
        self.name = S3
        self.state = state

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component.

        S3 integration is optional and purely relation-driven, so this component never
        blocks the charm on its own.
        """
        return [CharmStatuses.ACTIVE_IDLE.value]

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifests for the current s3-credentials relation."""
        if not (self.state.is_s3_related() and self.state.profile_config):
            return ReconciledManifests()

        connection_info = self.state.s3_connection_info
        access_key = connection_info.get("access-key")
        secret_key = connection_info.get("secret-key")
        bucket = connection_info.get("bucket")
        endpoint = connection_info.get("endpoint")
        region = connection_info.get("region")

        if not (access_key and secret_key and bucket and endpoint):
            self.logger.warning("S3 connection info is incomplete, skipping manifests generation")
            return ReconciledManifests()

        profile = self.state.profile_config.profile

        secrets_manifests = (
            [generate_minio_artifact_secret_manifest(profile, access_key, secret_key)]
            if self.state.is_k8s_secrets_manifests_related()
            else []
        )

        configmaps_manifests = []
        if self.state.is_k8s_configmaps_manifests_related():
            s3_config = self.state.s3_config
            default_pipeline_root = (
                s3_config.default_pipeline_root
                if s3_config and s3_config.default_pipeline_root
                else DEFAULT_PIPELINE_ROOT_TEMPLATE.format(bucket=bucket)
            )
            configmaps_manifests = [
                generate_artifact_repositories_configmap_manifest(profile, bucket, endpoint),
                generate_kfp_launcher_configmap_manifest(
                    profile, bucket, endpoint, region, default_pipeline_root
                ),
            ]

        return ReconciledManifests(secrets=secrets_manifests, configmaps=configmaps_manifests)
