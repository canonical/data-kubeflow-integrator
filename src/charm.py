#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

import ops
from data_platform_helpers.advanced_statuses.handler import StatusHandler

from core.state import GlobalState
from events.general import GeneralEventsHandler
from managers.kafka import KafkaManager
from managers.manifests import KubernetesManifestsManager
from managers.mongodb import MongodbManager
from managers.mysql import MysqlManager
from managers.opensearch import OpenSearchManager
from managers.postgresql import PostgresqlManager

logger = logging.getLogger(__name__)


class KubeflowIntegratorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, *args):
        super().__init__(*args)

        # Context
        self.state = GlobalState(self)

        # Managers
        self.opensearch_manager = OpenSearchManager(self.state)
        self.kafka_manager = KafkaManager(self.state)
        self.mysql_manager = MysqlManager(self.state)
        self.postgresql_manager = PostgresqlManager(self.state)
        self.mongodb_manager = MongodbManager(self.state)
        self.manifests_manager = KubernetesManifestsManager(self.state)

        self.status = StatusHandler(  # priority order
            self,
            self.manifests_manager,
            self.kafka_manager,
            self.opensearch_manager,
            self.mysql_manager,
            self.postgresql_manager,
            self.mongodb_manager,
        )

        # Event Handlers
        self.general_events = GeneralEventsHandler(self, self.state)


if __name__ == "__main__":  # pragma: nocover
    ops.main(KubeflowIntegratorCharm)
