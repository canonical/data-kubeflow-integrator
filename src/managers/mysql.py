#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for Mysql related tasks."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires, Scope
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from pydantic import ValidationError

from constants import MYSQL
from core.config import MysqlConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.database import DatabaseManager


class MysqlManager(DatabaseManager, ManagerStatusProtocol):
    """Manager for mysql relation."""

    def __init__(self, state: GlobalState):
        super().__init__(state, MYSQL)

    @property
    def database_requirer(self) -> DatabaseRequires:
        """Returns the DatabaseRequires from events handler."""
        return self.state.charm.general_events.mysql

    @property
    def database_config(self) -> MysqlConfig | None:
        """Returns the mysql config from the global state."""
        return self.state.mysql_config

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        if scope == "app":
            # Show error status on app only
            status_list = []

            database_config = None

            try:
                database_config = MysqlConfig(**self.state.charm.config)
            except ValidationError as err:
                self.logger.warning(str(err))

                # If opensearch is related
                if len(self.database_requirer.relations) > 0:
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
            if database_config and not self.is_database_related:
                # Block the charm since we need the integration with opensearch
                status_list.append(CharmStatuses.missing_integration_with_mysql())

            return status_list or [CharmStatuses.ACTIVE_IDLE.value]
        else:
            return [CharmStatuses.ACTIVE_IDLE.value]
