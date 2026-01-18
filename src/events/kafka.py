#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for Kafka related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    KafkaRequirerEventHandlers,
    TopicCreatedEvent,
    TopicEntityCreatedEvent,
)
from ops import Object, RelationBrokenEvent

from constants import (
    KAFKA_RELATION_NAME,
)
from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class KafkaEventsHandler(Object, WithLogging):
    """Class implementing Kafka events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="kafka")
        self.charm = charm
        self.state = state

        self.kafka = KafkaRequirerEventHandlers(self.charm, self.state.kafka_requirer)

        self.framework.observe(self.kafka.on.topic_created, self._on_topic_created)
        self.framework.observe(self.kafka.on.topic_entity_created, self._on_topic_entity_created)
        self.framework.observe(
            self.charm.on[KAFKA_RELATION_NAME].relation_broken, self._on_relation_broken
        )

    def _on_topic_created(self, event: TopicCreatedEvent) -> None:
        """Event triggered when a topic is created for this application."""
        self.logger.debug(f"Topic is created and credentials received {event.username}")
        self.charm.general_events._on_config_changed(event)

    def _on_topic_entity_created(self, event: TopicEntityCreatedEvent) -> None:
        """Event triggered when an entity is created for this application."""
        self.logger.debug(f"Entity credentials are received: {event.entity_name}")
        self.charm.general_events._on_config_changed(event)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle relation broken event."""
        pass

    def update_relation_data(self) -> None:
        """Update kafka relation data with latest config."""
        kafka_config = self.state.kafka_config
        if kafka_config:
            relation_data = {
                "topic": kafka_config.topic_name if kafka_config else "",
                "extra-user-roles": kafka_config.extra_user_roles or "",
                "consumer-group-prefix": kafka_config.consumer_group_prefix or "",
            }
            for rel in self.kafka.relations:
                self.kafka.update_relation_data(rel.id, relation_data)
