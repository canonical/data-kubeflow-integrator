#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

import ops
from data_platform_helpers.advanced_statuses.handler import StatusHandler

from core.state import GlobalState
from events.general import GeneralEventsHandler
from events.kafka import KafkaEventsHandler
from events.kfp_s3_storage import KFPS3StorageEventsHandler
from events.manifest import ManifestEventsHandler
from events.mongodb import MongoDBEventsHandler
from events.mysql import MySQLEventsHandler
from events.opensearch import OpenSearchEventsHandler
from events.postgresql import PostgresqlEventsHandler
from events.spark import SparkEventsHandler
from managers.kafka import KafkaManager
from managers.kfp_s3_storage import KFPS3StorageManager
from managers.mongodb import MongodbManager
from managers.mysql import MysqlManager
from managers.opensearch import OpenSearchManager
from managers.postgresql import PostgresqlManager
from managers.profile import KubeflowProfileManager
from managers.spark import SparkManager

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
        self.spark_manager = SparkManager(self.state)
        self.kfp_s3_manager = KFPS3StorageManager(self.state)
        self.profile_manager = KubeflowProfileManager(self.state)

        self.status = StatusHandler(  # priority order
            self,
            self.profile_manager,
            self.kafka_manager,
            self.opensearch_manager,
            self.mysql_manager,
            self.postgresql_manager,
            self.mongodb_manager,
            self.spark_manager,
            self.kfp_s3_manager,
        )

        # Event Handlers
        self.general_events = GeneralEventsHandler(self, self.state)
        self.kafka_events = KafkaEventsHandler(self, self.state)
        self.mongodb_events = MongoDBEventsHandler(self, self.state)
        self.mysql_events = MySQLEventsHandler(self, self.state)
        self.opensearch_events = OpenSearchEventsHandler(self, self.state)
        self.postgresql_events = PostgresqlEventsHandler(self, self.state)
        self.spark_events = SparkEventsHandler(self, self.state)
        self.kfp_s3_events = KFPS3StorageEventsHandler(self, self.state)
        self.manifest_events = ManifestEventsHandler(self, self.state)


if __name__ == "__main__":  # pragma: nocover
    ops.main(KubeflowIntegratorCharm)
