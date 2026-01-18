#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for mysql/postgresql/mongodb related tasks."""

from abc import ABC, abstractmethod

from charms.data_platform_libs.v0.data_interfaces import RequirerData
from charms.resource_dispatcher.v0.kubernetes_manifests import (
    KubernetesManifest,  # type: ignore[import-untyped]
)

from constants import (
    K8S_DATABASE_SECRET_NAME,
    K8S_DATABASE_TLS_CERT_PATH,
    K8S_DATABASE_TLS_SECRET_NAME,
)
from core.config import MongoDbConfig, MysqlConfig, PostgresqlConfig, OpenSearchConfig
from core.state import GlobalState
from managers.manifests import KubernetesManifestsManager
from utils.helpers_manifests import (
    generate_poddefault_manifest,
    generate_secret_manifest,
    generate_tls_secret_manifest,
)
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging

DatabaseConfig = MysqlConfig | PostgresqlConfig | MongoDbConfig | OpenSearchConfig


class DatabaseManager(WithLogging, ABC):
    """Abstract Manager for mysql/mongodb/postgresql relation."""

    def __init__(self, state: GlobalState, name: str):
        self.state = state
        self.name = name
        self.manifest_manager = KubernetesManifestsManager(state)

    @property
    @abstractmethod
    def database_config(self) -> DatabaseConfig | None:
        """Return the database config from state."""

    @property
    @abstractmethod
    def database_requirer(self) -> RequirerData:
        """Return the RequirerData instance from event handlers."""

    @property
    @abstractmethod
    def active_database(self) -> str | None:
        """Return the created and configured database."""

    @abstractmethod
    def is_database_related(self) -> bool:
        """Check if we have a relation with the database."""

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifesets for the current credentials."""
        if not (self.is_database_related() and self.active_database):
            return ReconciledManifests()

        if not self.state.profile_config:
            self.logger.warning("No specified profile, skipping manifests generation")
            return ReconciledManifests()

        # Fetch credentials
        creds = list(self.database_requirer.fetch_relation_data().values())[0]
        database_name = self.name

        database_secret: KubernetesManifest | None = None
        tls_secret: KubernetesManifest | None = None
        secrets_manifests: list[KubernetesManifest] = []
        poddefault_manifests: list[KubernetesManifest] = []

        # remove data field from the credentials
        if "data" in creds:
            creds.pop("data", None)

        # generate secrets manifests
        if self.state.is_k8s_secrets_manifests_related():
            secret_data = creds.copy()
            tls_secret = generate_tls_secret_manifest(
                self.manifest_manager.secret_k8s_template,
                self.state.profile_config.profile,
                creds,
                database_name,
            )
            if tls_secret:
                secrets_manifests.append(tls_secret)
                # Add a path to the mounted tls
                secret_data["ca_cert_path"] = K8S_DATABASE_TLS_CERT_PATH[database_name]
                # Remove "tls-ca" from creds
                secret_data.pop("tls-ca")

            database_secret = generate_secret_manifest(
                self.manifest_manager.secret_k8s_template,
                self.state.profile_config.profile,
                secret_data,
                database_name,
            )
            secrets_manifests.append(database_secret)

        # generate pod defaults
        if self.state.is_k8s_poddefaults_manifests_related():
            # If a tls-secret was generated, we include the cert path in pod default
            if tls_secret:
                creds["ca_cert_path"] = K8S_DATABASE_TLS_CERT_PATH[database_name]
                creds.pop("tls-ca")

            poddefault = generate_poddefault_manifest(
                self.manifest_manager.poddefault_k8s_template,
                self.state.profile_config.profile,
                creds,
                database_name,
                from_secret=K8S_DATABASE_SECRET_NAME[database_name] if database_secret else None,
                tls_secret=K8S_DATABASE_TLS_SECRET_NAME[database_name] if tls_secret else None,
            )
            poddefault_manifests.append(poddefault)
        return ReconciledManifests(secrets=secrets_manifests, poddefaults=poddefault_manifests)
