from pydantic import ValidationError
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires, Scope
from core.config import MysqlConfig, PostgresqlConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from managers.database import DatabaseManager
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol


class PostgresqlManager(DatabaseManager, ManagerStatusProtocol):
    def __init__(self, state: GlobalState):
        super().__init__(state)
        self.name = "postgresql"

    @property
    def database_requirer(self) -> DatabaseRequires:
        return self.state.charm.general_events.mysql

    @property
    def database_config(self) -> PostgresqlConfig | None:
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
                status_list.append(CharmStatuses.missing_integration_with_postgresql())

            return status_list or [CharmStatuses.ACTIVE_IDLE.value]
        else:
            return [CharmStatuses.ACTIVE_IDLE.value]
