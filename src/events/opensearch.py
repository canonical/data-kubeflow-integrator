#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for Opensearch related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    IndexCreatedEvent,
    IndexEntityCreatedEvent,
    OpenSearchRequiresEventHandlers,
)
from ops import Object, RelationBrokenEvent

from constants import (
    OPENSEARCH_RELATION_NAME,
)
from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class OpenSearchEventsHandler(Object, WithLogging):
    """Class implementing Opensearch events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="opensearch")

        self.charm = charm
        self.state = state

        self.opensearch = OpenSearchRequiresEventHandlers(
            self.charm, self.state.opensearch_requirer
        )

        self.framework.observe(self.opensearch.on.index_created, self._on_index_created)
        self.framework.observe(
            self.opensearch.on.index_entity_created, self._on_index_entity_created
        )
        self.framework.observe(
            self.charm.on[OPENSEARCH_RELATION_NAME].relation_broken, self._on_relation_broken
        )

    def _on_index_created(self, event: IndexCreatedEvent) -> None:
        """Event triggered when an index is created for this application."""
        self.logger.debug(f"OpenSearch credentials are received: {event.username}")
        self.charm.general_events._on_config_changed(event)

    def _on_index_entity_created(self, event: IndexEntityCreatedEvent) -> None:
        """Event triggered when an entity is created for this application."""
        self.logger.debug(f"Entity credentials are received: {event.entity_name}")
        self.charm.general_events._on_config_changed(event)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle relation broken event."""
        pass

    def update_relation_data(self) -> None:
        """Update opensearch relation data with latest config."""
        opensearch_config = self.state.opensearch_config
        if opensearch_config:
            relation_data = {
                "index": opensearch_config.index_name if opensearch_config else "",
                "extra-user-roles": opensearch_config.extra_user_roles or "",
            }
            for rel in self.opensearch.relations:
                self.opensearch.update_relation_data(rel.id, relation_data)
