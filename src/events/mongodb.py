#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for MongoDB related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseEntityCreatedEvent,
    DatabaseRequirerEventHandlers,
)
from ops import Object, RelationBrokenEvent

from constants import (
    MONGODB_RELATION_NAME,
)
from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class MongoDBEventsHandler(Object, WithLogging):
    """Class implementing MongoDB events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="mongodb")
        self.charm = charm
        self.state = state

        self.mongodb = DatabaseRequirerEventHandlers(self.charm, self.state.mongodb_requirer)

        self.framework.observe(self.mongodb.on.database_created, self._on_database_created)
        self.framework.observe(
            self.mongodb.on.database_entity_created, self._on_database_entity_created
        )
        self.framework.observe(
            self.charm.on[MONGODB_RELATION_NAME].relation_broken, self._on_relation_broken
        )

    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database is created for mongodb."""
        self.logger.debug(f"Database credentials are received: {event.username}")
        self.charm.general_events._on_config_changed(event)

    def _on_database_entity_created(self, event: DatabaseEntityCreatedEvent) -> None:
        """Event triggered when a database entity is created for mongodb."""
        self.logger.debug(f"Database entity credentials are received: {event.entity_name}")
        self.charm.general_events._on_config_changed(event)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle relation broken event."""
        pass

    def update_relation_data(self) -> None:
        """Update mongodb relation data with latest config."""
        if self.state.mongodb_config:
            relation_data = {
                "database": self.state.mongodb_config.database_name
                if self.state.mongodb_config
                else "",
                "extra-user-roles": self.state.mongodb_config.extra_user_roles or "",
            }
            for rel in self.mongodb.relations:
                self.mongodb.update_relation_data(rel.id, relation_data)
