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
)
from charms.data_platform_libs.v0.data_models import ValidationError
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    SparkServiceAccountRequirerData,
)
from data_platform_helpers.advanced_statuses.protocol import StatusesState, StatusesStateProtocol
from ops import Object, Relation

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

    # Relations

    @property
    def peer_relation(self) -> Relation | None:
        """Get the cluster peer relation."""
        return self.model.get_relation(PEER_RELATION)

    @property
    def opensearch_relation(self) -> Relation | None:
        """Get the opensearch relation."""
        return self.model.get_relation(OPENSEARCH_RELATION_NAME)

    @property
    def postgresql_relation(self) -> Relation | None:
        """Get the postgresql relation."""
        return self.model.get_relation(POSTGRESQL_RLEATION_NAME)

    @property
    def mysql_relation(self) -> Relation | None:
        """Get the mysql relation."""
        return self.model.get_relation(MYSQL_RELATION_NAME)

    # Charm Configurations

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

    # OpenSearch Relation
    @property
    def active_opensearch_index(self) -> str | None:
        """Return the created and configured opensearch index."""
        if (
            relation := self.opensearch_requirer.relations[0]
            if len(self.opensearch_requirer.relations)
            else None
        ):
            return self.opensearch_requirer.fetch_relation_field(relation.id, "index")
        return None

    def is_opensearch_related(self) -> bool:
        """Check if we have a relation with OpenSearch."""
        for relation in self.opensearch_requirer.relations:
            data = self.opensearch_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False

    # PostgreSQL Relation
    @property
    def active_postgresql_database(self) -> str | None:
        """Return the created and configured postgresql database."""
        if (
            relation := self.postgresql_requirer.relations[0]
            if len(self.postgresql_requirer.relations)
            else None
        ):
            return self.postgresql_requirer.fetch_relation_field(relation.id, "database")
        return None

    def is_postgresql_related(self) -> bool:
        """Check if we have a relation with PostgreSQL."""
        for relation in self.postgresql_requirer.relations:
            data = self.postgresql_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False

    # MySQL Relation
    @property
    def active_mysql_database(self) -> str | None:
        """Return the created and configured mysql database."""
        if (
            relation := self.mysql_requirer.relations[0]
            if len(self.mysql_requirer.relations)
            else None
        ):
            return self.mysql_requirer.fetch_relation_field(relation.id, "database")
        return None

    def is_mysql_related(self) -> bool:
        """Check if we have a relation with MySQL."""
        for relation in self.mysql_requirer.relations:
            data = self.mysql_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False

    # MongoDB Relation
    @property
    def active_mongodb_database(self) -> str | None:
        """Return the created and configured mongodb database."""
        if (
            relation := self.mongodb_requirer.relations[0]
            if len(self.mongodb_requirer.relations)
            else None
        ):
            return self.mongodb_requirer.fetch_relation_field(relation.id, "database")
        return None

    def is_mongodb_related(self) -> bool:
        """Check if we have a relation with MongoDB."""
        for relation in self.mongodb_requirer.relations:
            data = self.mongodb_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False

    # Kafka Relation
    @property
    def active_kafka_topic(self) -> str | None:
        """Return the created and configured kafka topic."""
        if (
            relation := self.kafka_requirer.relations[0]
            if len(self.kafka_requirer.relations)
            else None
        ):
            return self.kafka_requirer.fetch_relation_field(relation.id, "topic")
        return None

    def is_kafka_related(self) -> bool:
        """Check if we have a relation with Kafka."""
        for relation in self.kafka_requirer.relations:
            data = self.kafka_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False

    # Spark Relation
    def is_spark_related(self) -> bool:
        """Check if we have a relation with Kafka."""
        for relation in self.spark_requirer.relations:
            data = self.spark_requirer.fetch_relation_data(
                [relation.id], ["service-account", "resource-manifest", "spark-properties"]
            ).get(relation.id, {})
            if all(
                data.get(key)
                for key in ("service-account", "resource-manifest", "spark-properties")
            ):
                return True
        return False

    @property
    def active_spark_service_account(self) -> str | None:
        """Return the service account that was created and configured."""
        if (
            relation := self.spark_requirer.relations[0]
            if len(self.spark_requirer.relations)
            else None
        ):
            sa_with_namespace = self.spark_requirer.fetch_relation_field(
                relation.id, "service-account"
            )
            if not sa_with_namespace:
                return None
            parts = sa_with_namespace.split(":")
            if len(parts) != 2:
                return None
            _, service_account = parts
            return service_account
        return None

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
