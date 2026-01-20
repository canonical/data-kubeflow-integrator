#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Object representing the state of KubeflowIntegrator Charm."""

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseRequirerData,
    DataPeerData,
    DataPeerUnitData,
    KafkaRequirerData,
    OpenSearchRequiresData,
    RequirerData,
)
from charms.data_platform_libs.v0.data_models import ValidationError
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    SparkServiceAccountRequirerData,
)
from data_platform_helpers.advanced_statuses.protocol import StatusesState, StatusesStateProtocol
from ops import Object

from constants import (
    KAFKA_RELATION_NAME,
    MONGODB_RELATION_NAME,
    MYSQL_RELATION_NAME,
    OPENSEARCH_RELATION_NAME,
    PEER_RELATION,
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
    POSTGRESQL_RLEATION_NAME,
    ROLEBINDINGS_DISPATCHER_RELATION_NAME,
    ROLES_DISPATCHER_RELATION_NAME,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
    SPARK_RELATION_NAME,
    STATUS_PEERS_RELATION_NAME,
)
from core.config import (
    KafkaConfig,
    MongoDbConfig,
    MysqlConfig,
    OpenSearchConfig,
    PostgresqlConfig,
    ProfileConfig,
    SparkConfig,
)
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class GlobalState(Object, WithLogging, StatusesStateProtocol):
    """Properties and relations of the charm."""

    def __init__(self, charm: "KubeflowIntegratorCharm"):
        super().__init__(charm, "charm_state")
        self.charm = charm
        self.config = charm.config

        self.peer_app_interface = DataPeerData(
            self.model,
            relation_name=PEER_RELATION,
        )
        self.peer_unit_interface = DataPeerUnitData(self.model, relation_name=PEER_RELATION)
        self.opensearch_requirer = OpenSearchRequiresData(
            self.charm.model,
            OPENSEARCH_RELATION_NAME,
            getattr(self.opensearch_config, "index_name", ""),
            extra_user_roles=getattr(self.opensearch_config, "extra_user_roles", ""),
        )
        self.postgresql_requirer = DatabaseRequirerData(
            self.charm.model,
            relation_name=POSTGRESQL_RLEATION_NAME,
            database_name=getattr(self.postgresql_config, "database_name", ""),
            extra_user_roles=getattr(self.postgresql_config, "extra_user_roles", ""),
        )
        self.mysql_requirer = DatabaseRequirerData(
            self.charm.model,
            relation_name=MYSQL_RELATION_NAME,
            database_name=getattr(self.mysql_config, "database_name", ""),
            extra_user_roles=getattr(self.mysql_config, "extra_user_roles", ""),
        )
        self.mongodb_requirer = DatabaseRequirerData(
            self.charm.model,
            relation_name=MONGODB_RELATION_NAME,
            database_name=getattr(self.mongodb_config, "database_name", ""),
            extra_user_roles=getattr(self.mongodb_config, "extra_user_roles", ""),
        )
        self.kafka_requirer = KafkaRequirerData(
            self.charm.model,
            relation_name=KAFKA_RELATION_NAME,
            topic=getattr(self.kafka_config, "topic_name", ""),
            extra_user_roles=getattr(self.kafka_config, "extra_user_roles", ""),
            consumer_group_prefix=getattr(self.kafka_config, "consumer_group_prefix", ""),
        )

        profile = getattr(self.profile_config, "profile", "")
        namespace = profile if profile != "*" else self.charm.model.name
        username = getattr(self.spark_config, "spark_service_account", "")
        self.spark_requirer = SparkServiceAccountRequirerData(
            self.charm.model,
            relation_name=SPARK_RELATION_NAME,
            service_account=f"{namespace}:{username}",
            skip_creation=True,
        )

        self.statuses = StatusesState(self, STATUS_PEERS_RELATION_NAME)

    @property
    def opensearch_config(self) -> OpenSearchConfig | None:
        """Return information regarding opensearch config options."""
        try:
            return OpenSearchConfig(**self.config)
        except Exception:
            return None

    @property
    def postgresql_config(self) -> PostgresqlConfig | None:
        """Return current configuration of postgresql."""
        try:
            return PostgresqlConfig(**self.config)
        except Exception:
            return None

    @property
    def mysql_config(self) -> MysqlConfig | None:
        """Return current configuration of mysql."""
        try:
            return MysqlConfig(**self.config)
        except Exception:
            return None

    @property
    def mongodb_config(self) -> MongoDbConfig | None:
        """Return current configuration of mongodb."""
        try:
            return MongoDbConfig(**self.config)
        except Exception:
            return None

    @property
    def kafka_config(self) -> KafkaConfig | None:
        """Return current configuration kafka."""
        try:
            return KafkaConfig(**self.config)
        except Exception:
            return None

    @property
    def spark_config(self) -> SparkConfig | None:
        """Return current configuration related to Spark."""
        try:
            return SparkConfig(**self.config)
        except Exception:
            return None

    @property
    def profile_config(self) -> ProfileConfig | None:
        """Return information regarding the profile config option."""
        try:
            return ProfileConfig(**self.config)
        except ValidationError:
            return None

    def _get_field(self, relation_data: RequirerData, field: str) -> str | None:
        """Get a field from the first relation if it exists."""
        if relation := relation_data.relations[0] if len(relation_data.relations) else None:
            return relation_data.fetch_relation_field(relation.id, field)
        return None

    def is_relation_ready(
        self, relation_data: RequirerData, required_fields: list[str] | None = None
    ) -> bool:
        """Check if we have a relation with a database."""
        required_fields = required_fields or ["username", "password"]
        for relation in relation_data.relations:
            data = relation_data.fetch_relation_data([relation.id], required_fields).get(
                relation.id, {}
            )
            if all(data.get(key) for key in required_fields):
                return True
        return False

    @property
    def active_opensearch_index(self) -> str | None:
        """Return the created and configured opensearch index."""
        return self._get_field(self.opensearch_requirer, "index")

    @property
    def active_postgresql_database(self) -> str | None:
        """Return the created and configured postgresql database."""
        return self._get_field(self.postgresql_requirer, "database")

    @property
    def active_mysql_database(self) -> str | None:
        """Return the created and configured mysql database."""
        return self._get_field(self.mysql_requirer, "database")

    @property
    def active_mongodb_database(self) -> str | None:
        """Return the created and configured mongodb database."""
        return self._get_field(self.mongodb_requirer, "database")

    @property
    def active_kafka_topic(self) -> str | None:
        """Return the created and configured kafka topic."""
        return self._get_field(self.kafka_requirer, "topic")

    @property
    def active_spark_service_account(self) -> str | None:
        """Return the service account that was created and configured."""
        sa_with_namespace = self._get_field(self.spark_requirer, "service-account")
        if not sa_with_namespace:
            return None
        parts = sa_with_namespace.split(":")
        if len(parts) != 2:
            return None
        _, service_account = parts
        return service_account

    def is_opensearch_related(self) -> bool:
        """Check if we have a relation with OpenSearch."""
        return self.is_relation_ready(self.opensearch_requirer)

    def is_postgresql_related(self) -> bool:
        """Check if we have a relation with PostgreSQL."""
        return self.is_relation_ready(self.postgresql_requirer)

    def is_mysql_related(self) -> bool:
        """Check if we have a relation with MySQL."""
        return self.is_relation_ready(self.mysql_requirer)

    def is_mongodb_related(self) -> bool:
        """Check if we have a relation with MongoDB."""
        return self.is_relation_ready(self.mongodb_requirer)

    def is_kafka_related(self) -> bool:
        """Check if we have a relation with Kafka."""
        return self.is_relation_ready(self.kafka_requirer)

    def is_spark_related(self) -> bool:
        """Check if we have a relation with Kafka."""
        return self.is_relation_ready(
            self.spark_requirer, ["service-account", "resource-manifest", "spark-properties"]
        )

    def is_k8s_secrets_manifests_related(self) -> bool:
        """Is the charm related to a secrets manifests relation."""
        return bool(self.charm.model.relations.get(SECRETS_DISPATCHER_RELATION_NAME))

    def is_k8s_poddefaults_manifests_related(self) -> bool:
        """Is the charm related to a poddefaults manifests relation."""
        return bool(self.charm.model.relations.get(POD_DEFAULTS_DISPATCHER_RELATION_NAME))

    def is_k8s_service_accounts_manifests_related(self) -> bool:
        """Is the charm related to a serviceaccounts manifests relation."""
        return bool(self.charm.model.relations.get(SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME))

    def is_k8s_roles_manifests_related(self) -> bool:
        """Is the charm related to a roles manifests relation."""
        return bool(self.charm.model.relations.get(ROLES_DISPATCHER_RELATION_NAME))

    def is_k8s_rolebindings_manifests_related(self) -> bool:
        """Is the charm related to a rolebindings manifests relation."""
        return bool(self.charm.model.relations.get(ROLEBINDINGS_DISPATCHER_RELATION_NAME))

    def is_manifests_provider_related(self):
        """Is the charm related to any manifests relation provider."""
        return any(
            [
                self.is_k8s_poddefaults_manifests_related(),
                self.is_k8s_secrets_manifests_related(),
                self.is_k8s_service_accounts_manifests_related(),
                self.is_k8s_roles_manifests_related(),
                self.is_k8s_rolebindings_manifests_related(),
            ]
        )
