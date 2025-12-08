#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for MongoDB related tasks."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires, Scope
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from pydantic import ValidationError

from constants import MONGODB
from core.config import MongoDbConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.database import DatabaseManager


class MongodbManager(DatabaseManager, ManagerStatusProtocol):
    """Manager for mongodb relation."""

    def __init__(self, state: GlobalState):
        super().__init__(state, MONGODB)

    @property
    def database_requirer(self) -> DatabaseRequires:
        """Returns DatabaseRequires from events handler."""
        return self.state.charm.general_events.mongodb

    @property
    def database_config(self) -> MongoDbConfig | None:
        """Returns mongodb config from the global state."""
        return self.state.mongodb_config

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        status_list = []
        database_config = None

        try:
            database_config = MongoDbConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.warning(str(err))

            # If opensearch is related
            if len(self.database_requirer.relations) > 0:
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
        if database_config and not self.is_database_related:
            # Block the charm since we need the integration with opensearch
            status_list.append(CharmStatuses.missing_integration_with_mongodb())

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]
