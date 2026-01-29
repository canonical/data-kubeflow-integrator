#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for Spark related tasks."""

from typing import Callable, cast

import yaml
from charms.resource_dispatcher.v0.kubernetes_manifests import (
    KubernetesManifest,  # type: ignore[import-untyped]
)
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from constants import (
    SPARK,
    SPARK_BLOCK_MANAGER_PORT,
    SPARK_DRIVER_PORT,
    SPARK_NOTEBOOK_PODDEFAULT_DESC,
    SPARK_NOTEBOOK_PODDEFAULT_NAME,
    SPARK_NOTEBOOK_PODDEFAULT_SELECTOR_LABEL,
    SPARK_PIPELINE_PODDEFAULT_DESC,
    SPARK_PIPELINE_PODDEFAULT_NAME,
    SPARK_PIPELINE_PODDEFAULT_SELECTOR_LABEL,
    SPARK_RELATION_NAME,
)
from core.config import SparkConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.manifests import KubernetesManifestsManager
from utils.helpers_manifests import generate_poddefault_manifest
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


class SparkManager(ManagerStatusProtocol, WithLogging):
    """Manager for spark relation."""

    def __init__(self, state: GlobalState):
        self.name = SPARK
        self.state = state
        self.manifest_manager = KubernetesManifestsManager(state)

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        status_list = []
        spark_config = None

        try:
            spark_config = SparkConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.warning(str(err))

            # If Spark is related
            if len(self.state.spark_requirer.relations) > 0:
                missing = [
                    str(error["loc"][0]) for error in err.errors() if error["type"] == "missing"
                ]
                invalid = [
                    str(error["loc"][0]) for error in err.errors() if error["type"] != "missing"
                ]

                if missing:
                    status_list.append(ConfigStatuses.missing_config_parameters(fields=missing))
                if invalid:
                    status_list.append(ConfigStatuses.invalid_config_parameters(fields=invalid))

        if spark_config and not self.state.is_spark_related():
            # Block the charm since we need the integration with spark
            status_list.append(CharmStatuses.missing_integration_with_spark())

        if (
            self.state.is_spark_related()
            and spark_config
            and self.state.active_spark_service_account != spark_config.spark_service_account
        ):
            status_list.append(
                ConfigStatuses.config_change_requires_relation_recreation(
                    config_option="spark-service-account", relation_name=SPARK_RELATION_NAME
                )
            )
        return status_list or [CharmStatuses.ACTIVE_IDLE.value]

    def _patch_rolebinding_subject_namespace(self, rolebinding: dict) -> dict:
        """Patch the namespace of the subject in the rolebinding."""
        if len(rolebinding.get("subjects", [])) == 0:
            return rolebinding

        for subject in rolebinding["subjects"]:
            if "namespace" not in subject:
                continue
            subject["namespace"] = "{{ NAMESPACE }}"

        return rolebinding

    def filter_manifests(
        self,
        manifest_yaml: list[dict],
        kind: str,
        manifest_relation_exists: bool,
        patch_function: Callable | None = None,
    ) -> list[KubernetesManifest]:
        """Filter the manifests of given kind from the given list of resource objects."""
        if not manifest_relation_exists:
            return []

        return [
            KubernetesManifest(
                manifest_content=yaml.dump(patch_function(res) if patch_function else res)
            )
            for res in manifest_yaml
            if res["kind"] == kind
        ]

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifests for the current spark relation."""
        if not (self.state.is_spark_related() and self.state.active_spark_service_account):
            return ReconciledManifests()

        # Fetch manifest from the relation
        relation = self.state.spark_requirer.relations[0]
        raw_manifests = list(self.state.spark_requirer.fetch_relation_data().values())[0][
            "resource-manifest"
        ]
        _, service_account = cast(
            str,
            self.state.spark_requirer.fetch_relation_field(
                relation_id=relation.id, field="service-account"
            ),
        ).split(":")

        if not self.state.profile_config:
            self.logger.warning("No specified profile, skipping manifests generation")
            return ReconciledManifests()

        manifest_yaml = list(yaml.safe_load_all(raw_manifests))

        secrets_manifests = self.filter_manifests(
            manifest_yaml, "Secret", self.state.is_k8s_secrets_manifests_related()
        )
        service_accounts_manifest = self.filter_manifests(
            manifest_yaml, "ServiceAccount", self.state.is_k8s_service_accounts_manifests_related()
        )
        roles_manifest = self.filter_manifests(
            manifest_yaml, "Role", self.state.is_k8s_roles_manifests_related()
        )
        rolebindings_manifest = self.filter_manifests(
            manifest_yaml,
            "RoleBinding",
            self.state.is_k8s_rolebindings_manifests_related(),
            patch_function=self._patch_rolebinding_subject_namespace,
        )

        spark_pipeline_poddefault = generate_poddefault_manifest(
            self.manifest_manager.poddefault_k8s_template,
            self.state.profile_config.profile,
            creds={
                "SPARK_SERVICE_ACCOUNT": service_account,
                "PYSPARK_SUBMIT_ARGS": (
                    f" --conf spark.driver.port={SPARK_DRIVER_PORT}"
                    f" --conf spark.blockManager.port={SPARK_BLOCK_MANAGER_PORT}"
                    f" --conf spark.kubernetes.executor.annotation.traffic.sidecar.istio.io/excludeInboundPorts={SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}"
                    f" --conf spark.kubernetes.executor.annotation.traffic.sidecar.istio.io/excludeOutboundPorts={SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}"
                    " pyspark-shell"
                ),
            },
            database_name=SPARK,
            poddefault_name=SPARK_PIPELINE_PODDEFAULT_NAME,
            poddefault_description=SPARK_PIPELINE_PODDEFAULT_DESC,
            annotations={
                "traffic.sidecar.istio.io/excludeInboundPorts": f"{SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}",
                "traffic.sidecar.istio.io/excludeOutboundPorts": f"{SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}",
            },
            fieldrefs={"SPARK_NAMESPACE": "metadata.namespace"},
            selector_name=SPARK_PIPELINE_PODDEFAULT_SELECTOR_LABEL,
        )
        spark_notebook_poddefault = generate_poddefault_manifest(
            self.manifest_manager.poddefault_k8s_template,
            self.state.profile_config.profile,
            creds={"SPARK_SERVICE_ACCOUNT": service_account},
            database_name=SPARK,
            poddefault_name=SPARK_NOTEBOOK_PODDEFAULT_NAME,
            poddefault_description=SPARK_NOTEBOOK_PODDEFAULT_DESC,
            args=[
                "--namespace",
                "$SPARK_NAMESPACE",
                "--username",
                "$SPARK_SERVICE_ACCOUNT",
                "--conf",
                f"spark.driver.port={SPARK_DRIVER_PORT}",
                "--conf",
                f"spark.blockManager.port={SPARK_BLOCK_MANAGER_PORT}",
                "--conf",
                f"spark.kubernetes.executor.annotation.traffic.sidecar.istio.io/excludeInboundPorts={SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}",
                "--conf",
                f"spark.kubernetes.executor.annotation.traffic.sidecar.istio.io/excludeOutboundPorts={SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}",
            ],
            annotations={
                "traffic.sidecar.istio.io/excludeInboundPorts": f"{SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}",
                "traffic.sidecar.istio.io/excludeOutboundPorts": f"{SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}",
            },
            fieldrefs={"SPARK_NAMESPACE": "metadata.namespace"},
            selector_name=SPARK_NOTEBOOK_PODDEFAULT_SELECTOR_LABEL,
        )
        poddefaults_manifest = (
            [spark_pipeline_poddefault, spark_notebook_poddefault]
            if self.state.is_k8s_poddefaults_manifests_related()
            else []
        )

        return ReconciledManifests(
            secrets=secrets_manifests,
            poddefaults=poddefaults_manifest,
            serviceaccounts=service_accounts_manifest,
            roles=roles_manifest,
            role_bindings=rolebindings_manifest,
        )
