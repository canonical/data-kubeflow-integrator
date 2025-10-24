#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for Postgresql related tasks."""

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires, Scope
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from pydantic import ValidationError

from constants import POSTGRESQL
from core.config import PostgresqlConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.database import DatabaseManager


class PostgresqlManager(DatabaseManager, ManagerStatusProtocol):
    """Manager for postgresql relation."""

    def __init__(self, state: GlobalState):
        super().__init__(state, POSTGRESQL)

    @property
    def database_requirer(self) -> DatabaseRequires:
        """Return Databaserequires from events handler."""
        return self.state.charm.general_events.postgresql

    @property
    def database_config(self) -> PostgresqlConfig | None:
        """Return postgresql config from global state."""
        return self.state.postgresql_config

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        if scope == "app":
            # Show error status on app only
            status_list = []

            database_config = None

            try:
                database_config = PostgresqlConfig(**self.state.charm.config)
            except ValidationError as err:
                self.logger.warning(str(err))

                # If postgresql is related
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
                # Block the charm since we need the integration with postgresql
                status_list.append(CharmStatuses.missing_integration_with_postgresql())

            return status_list or [CharmStatuses.ACTIVE_IDLE.value]
        else:
            return [CharmStatuses.ACTIVE_IDLE.value]
