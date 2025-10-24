#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for kafka related tasks."""

from charms.data_platform_libs.v0.data_interfaces import KafkaRequires
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from constants import KAFKA
from core.config import KafkaConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


class KafkaManager(ManagerStatusProtocol, WithLogging):
    """Manager for Kafka relation."""

    def __init__(self, state: GlobalState):
        self.name = KAFKA
        self.state = state

    def update_relation_data(self) -> None:
        """Update kafka relation data with latest config."""
        kafka_config = self.state.kafka_config
        if kafka_config:
            relation_data = {
                "topic": kafka_config.topic_name if kafka_config else "",
                "extra-user-roles": kafka_config.extra_user_roles or "",
                "consumer-group-prefix": kafka_config.consumer_group_prefix or "",
            }
            for rel in self.kafka_requirer.relations:
                self.kafka_requirer.update_relation_data(rel.id, relation_data)

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifesets for the current credentials."""
        if self.is_kafka_related and self.topic_active:
            # Fetch credentials
            kafka_creds = list(self.kafka_requirer.fetch_relation_data().values())[0]
            return self.state.charm.manifests_manager.reconcile_database_manifests(
                kafka_creds, KAFKA
            )
        return ReconciledManifests()

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        if scope == "app":
            # Show error status on app only
            status_list = []

            kafka_config = None

            try:
                kafka_config = KafkaConfig(**self.state.charm.config)
            except ValidationError as err:
                self.logger.warning(str(err))

                # If kafka is related
                if len(self.kafka_requirer.relations) > 0:
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
            if kafka_config and not self.is_kafka_related:
                # Block the charm since we need the integration with kafka
                status_list.append(CharmStatuses.missing_integration_with_kafka())

            return status_list or [CharmStatuses.ACTIVE_IDLE.value]
        else:
            return [CharmStatuses.ACTIVE_IDLE.value]

    @property
    def kafka_requirer(self) -> KafkaRequires:
        """Return the KafkaRequires instance from event handlers."""
        return self.state.charm.general_events.kafka

    @property
    def topic_active(self) -> str | None:
        """Return the created and configured kafka topic."""
        if (
            relation := self.kafka_requirer.relations[0]
            if len(self.kafka_requirer.relations)
            else None
        ):
            return self.kafka_requirer.fetch_relation_field(relation.id, "topic")
        return None

    @property
    def is_kafka_related(self) -> bool:
        """Check if we have a relation with Kafka."""
        for relation in self.kafka_requirer.relations:
            data = self.kafka_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False
