#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for kafka related tasks."""

from typing import cast

from charms.spark_integration_hub_k8s.v0.spark_service_account import SparkServiceAccountRequirer
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from constants import SPARK
from core.config import SparkConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


class SparkManager(ManagerStatusProtocol, WithLogging):
    """Manager for spark relation."""

    def __init__(self, state: GlobalState):
        self.name = SPARK
        self.state = state

    def update_relation_data(self) -> None:
        """Update spark relation data with latest config."""
        spark_config = self.state.spark_config
        if spark_config:
            relation_data = {
                "service-account": spark_config.spark_service_account,
                "skip-creation": "true",
            }
            for rel in self.spark_requirer.relations:
                self.spark_requirer.update_relation_data(rel.id, relation_data)

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifests for the current spark relation."""
        if self.is_spark_related and self.service_account_active:
            # Fetch manifest from the relation
            relation = self.spark_requirer.relations[0]
            raw_manifest = list(self.spark_requirer.fetch_relation_data().values())[0][
                "resource-manifest"
            ]
            namespace, service_account = cast(
                str,
                self.spark_requirer.fetch_relation_field(
                    relation_id=relation.id, field="service-account"
                ),
            ).split(":")

            return self.state.charm.manifests_manager.reconcile_spark_manifests(
                raw_manifest, namespace, service_account
            )
        return ReconciledManifests()

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        if scope == "app":
            # Show error status on app only
            status_list = []
            spark_config = None

            try:
                spark_config = SparkConfig(**self.state.charm.config)
            except ValidationError as err:
                self.logger.warning(str(err))

                # If Spark is related
                if len(self.spark_requirer.relations) > 0:
                    missing = [
                        str(error["loc"][0])
                        for error in err.errors()
                        if error["type"] == "missing"
                    ]
                    invalid = [
                        str(error["loc"][0])
                        for error in err.errors()
                        if error["type"] != "missing"
                    ]

                    if missing:
                        status_list.append(
                            ConfigStatuses.missing_config_parameters(fields=missing)
                        )
                    if invalid:
                        status_list.append(
                            ConfigStatuses.invalid_config_parameters(fields=invalid)
                        )
            if spark_config and not self.is_spark_related:
                # Block the charm since we need the integration with spark
                status_list.append(CharmStatuses.missing_integration_with_kafka())

            return status_list or [CharmStatuses.ACTIVE_IDLE.value]
        else:
            return [CharmStatuses.ACTIVE_IDLE.value]

    @property
    def spark_requirer(self) -> SparkServiceAccountRequirer:
        """Return the SparkServiceAccountRequirer instance from event handlers."""
        return self.state.charm.general_events.spark

    @property
    def service_account_active(self) -> str | None:
        """Return the service account that was created and configured."""
        if (
            relation := self.spark_requirer.relations[0]
            if len(self.spark_requirer.relations)
            else None
        ):
            return self.spark_requirer.fetch_relation_field(relation.id, "service-account")
        return None

    @property
    def is_spark_related(self) -> bool:
        """Check if we have a relation with Kafka."""
        for relation in self.spark_requirer.relations:
            data = self.spark_requirer.fetch_relation_data(
                [relation.id], ["service-account", "resource-manifest", "spark-properties"]
            ).get(relation.id, {})
            if all(
                data.get(key)
                for key in ("service-account", "resource-manifest", "spark-properties")
            ):
                return True
        return False
