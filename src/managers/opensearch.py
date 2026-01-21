#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for opensearch related tasks."""

from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequiresData
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from constants import OPENSEARCH
from core.config import OpenSearchConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.database import DatabaseManager
from utils.logging import WithLogging


class OpenSearchManager(DatabaseManager, ManagerStatusProtocol, WithLogging):
    """Manager for Opensearch relation."""

    def __init__(self, state: GlobalState):
        super().__init__(state, OPENSEARCH)

    @property
    def database_requirer(self) -> OpenSearchRequiresData:
        """Return opensearch_requirer data component."""
        return self.state.opensearch_requirer

    @property
    def database_config(self) -> OpenSearchConfig | None:
        """Return Opensearch config from global state."""
        return self.state.opensearch_config

    @property
    def active_database(self) -> str | None:
        """Return the created and configured index."""
        return self.state.active_opensearch_index

    def is_database_related(self) -> bool:
        """Check if we have a relation with opensearch."""
        return self.state.is_opensearch_related()

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        status_list = []
        opensearch_config = None

        try:
            opensearch_config = OpenSearchConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.warning(str(err))

            # If opensearch is related
            if len(self.state.opensearch_requirer.relations) > 0:
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
        if opensearch_config and not self.state.is_opensearch_related():
            # Block the charm since we need the integration with opensearch
            status_list.append(CharmStatuses.missing_integration_with_opensearch())

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]
