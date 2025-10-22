#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for mysql/postgresql/mongodb related tasks."""

from abc import ABC, abstractmethod
from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires

from core.config import MongoDbConfig, MysqlConfig, PostgresqlConfig
from core.state import GlobalState
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


DatabaseConfig = MysqlConfig | PostgresqlConfig | MongoDbConfig


class DatabaseManager(WithLogging, ABC):
    """Abstract Manager for mysql/mongodb/postgresql relation."""

    def __init__(self, state: GlobalState):
        self.state = state

    def update_relation_data(self) -> None:
        """Update opensearch relation data with latest config."""
        if self.database_config:
            relation_data = {
                "database": self.database_config.database_name if self.database_config else "",
                "extra-user-roles": self.database_config.extra_user_roles or "",
            }
            for rel in self.database_requirer.relations:
                self.database_requirer.update_relation_data(rel.id, relation_data)

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifesets for the current credentials."""
        if self.is_database_related and self.database_active:
            # Fetch credentials
            opensearch_creds = list(self.database_requirer.fetch_relation_data().values())[0]
            return self.state.charm.manifests_manager.reconcile_opensearch_manifests(
                opensearch_creds
            )
        return ReconciledManifests()

    @property
    @abstractmethod
    def database_config(self) -> DatabaseConfig | None:
        """Return the database config from state."""

    @property
    @abstractmethod
    def database_requirer(self) -> DatabaseRequires:
        """Return the databaseRequires instance from event handlers."""

    @property
    def database_active(self) -> str | None:
        """Return the created and configured database."""
        if (
            relation := self.database_requirer.relations[0]
            if len(self.database_requirer.relations)
            else None
        ):
            return self.database_requirer.fetch_relation_field(relation.id, "database")
        return None

    @property
    def is_database_related(self) -> bool:
        """Check if we have a relation with the database."""
        for relation in self.database_requirer.relations:
            data = self.database_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False
