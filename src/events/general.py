#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for general charm lifecycle events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ops import Object
from ops.charm import ConfigChangedEvent

from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class GeneralEventsHandler(Object, WithLogging):
    """Class implementing general charm lifecycle events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="general")

        self.charm = charm
        self.state = state

        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:
        """Event handler for configuration changed events."""
        # Only execute in the unit leader
        if not self.charm.unit.is_leader():
            return

        self.logger.debug(f"Config changed... Current configuration: {self.charm.config}")

        if self.state.opensearch_config and not self.state.active_opensearch_index:
            # route the config change to appropriate handler
            self.charm.opensearch_events.update_relation_data()

        if self.state.postgresql_config and not self.state.active_postgresql_database:
            # route the config change to postgresql manager
            self.charm.postgresql_events.update_relation_data()

        if self.state.mysql_config and not self.state.active_mysql_database:
            # route the config change to mysql manager
            self.charm.mysql_events.update_relation_data()

        if self.state.mongodb_config and not self.state.active_mongodb_database:
            # route the config change to mongodb manager
            self.charm.mongodb_events.update_relation_data()

        if self.state.kafka_config and not self.state.active_kafka_topic:
            # route the config change to kafka manager
            self.charm.kafka_events.update_relation_data()

        if self.state.spark_config:
            # route the config change to spark manager
            self.charm.spark_events.update_relation_data()

        # reconcile manifests
        self.charm.manifest_events._on_manifests_relation_change(event)
