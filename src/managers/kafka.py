#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for kafka related tasks."""

from charms.data_platform_libs.v0.data_interfaces import KafkaRequirerData
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from constants import KAFKA
from core.config import KafkaConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.database import DatabaseManager


class KafkaManager(DatabaseManager, ManagerStatusProtocol):
    """Manager for Kafka relation."""

    def __init__(self, state: GlobalState):
        super().__init__(state, KAFKA)

    @property
    def database_requirer(self) -> KafkaRequirerData:
        """Return kafka_requirer data component."""
        return self.state.kafka_requirer

    @property
    def database_config(self) -> KafkaConfig | None:
        """Return Kafka config from global state."""
        return self.state.kafka_config

    @property
    def active_database(self) -> str | None:
        """Return the created and configured index."""
        return self.state.active_kafka_topic

    def is_database_related(self) -> bool:
        """Check if we have a relation with kafka."""
        return self.state.is_kafka_related()

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        # Show error status on app only
        status_list = []
        kafka_config = None

        try:
            kafka_config = KafkaConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.warning(str(err))

            # If kafka is related
            if len(self.state.kafka_requirer.relations) > 0:
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
        if kafka_config and not self.state.is_kafka_related():
            # Block the charm since we need the integration with kafka
            status_list.append(CharmStatuses.missing_integration_with_kafka())

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]
