#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for MySQL related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseEntityCreatedEvent,
    DatabaseRequirerEventHandlers,
)
from ops import Object, RelationBrokenEvent

from constants import (
    MYSQL_RELATION_NAME,
)
from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class MySQLEventsHandler(Object, WithLogging):
    """Class implementing MySQL events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="mysql")
        self.charm = charm
        self.state = state

        self.mysql = DatabaseRequirerEventHandlers(self.charm, self.state.mysql_requirer)

        self.framework.observe(self.mysql.on.database_created, self._on_database_created)
        self.framework.observe(
            self.mysql.on.database_entity_created, self._on_database_entity_created
        )
        self.framework.observe(
            self.charm.on[MYSQL_RELATION_NAME].relation_broken, self._on_relation_broken
        )

    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database is created for mysql."""
        self.logger.debug(f"Database credentials are received: {event.username}")
        self.charm.general_events._on_config_changed(event)

    def _on_database_entity_created(self, event: DatabaseEntityCreatedEvent) -> None:
        """Event triggered when a database entity is created for mysql."""
        self.logger.debug(f"Database entity credentials are received: {event.entity_name}")
        self.charm.general_events._on_config_changed(event)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle relation broken event."""
        pass

    def update_relation_data(self) -> None:
        """Update mysql relation data with latest config."""
        if self.state.mysql_config:
            relation_data = {
                "database": self.state.mysql_config.database_name
                if self.state.mysql_config
                else "",
                "extra-user-roles": self.state.mysql_config.extra_user_roles or "",
            }
            for rel in self.state.mysql_requirer.relations:
                self.mysql.update_relation_data(rel.id, relation_data)
