#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for General Kubeflow-integrator charm events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DatabaseCreatedEvent,
    DatabaseEntityCreatedEvent,
    DatabaseRequires,
    IndexCreatedEvent,
    IndexEntityCreatedEvent,
    KafkaRequires,
    OpenSearchRequires,
    TopicCreatedEvent,
    TopicEntityCreatedEvent,
)
from charms.resource_dispatcher.v0.kubernetes_manifests import (
    KubernetesManifestRequirerWrapper,  # type: ignore
)
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    ServiceAccountPropertyChangedEvent,
    SparkServiceAccountRequirer,
)
from ops import EventBase, Object, RelationBrokenEvent

from constants import (
    KAFKA_RELATION_NAME,
    MONGODB_RELATION_NAME,
    MYSQL_RELATION_NAME,
    OPENSEARCH_RELATION_NAME,
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
    POSTGRESQL_RLEATION_NAME,
    ROLEBINDINGS_DISPATCHER_RELATION_NAME,
    ROLES_DISPATCHER_RELATION_NAME,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
    SPARK_RELATION_NAME,
)
from core.state import GlobalState
from managers.manifests import ReconciledManifests
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class GeneralEventsHandler(Object, WithLogging):
    """Class implementing Kubeflow-integrator events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="general")

        self.charm = charm
        self.state = state

        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

        ## databases
        self.opensearch = OpenSearchRequires(
            self.charm,
            OPENSEARCH_RELATION_NAME,
            getattr(self.state.opensearch_config, "index_name", ""),
            extra_user_roles=getattr(self.state.opensearch_config, "extra_user_roles", ""),
        )
        self.kafka = KafkaRequires(
            self.charm,
            relation_name=KAFKA_RELATION_NAME,
            topic=getattr(self.state.kafka_config, "topic_name", ""),
            extra_user_roles=getattr(self.state.kafka_config, "extra_user_roles", ""),
            consumer_group_prefix=getattr(self.state.kafka_config, "consumer_group_prefix", ""),
        )

        self.postgresql = DatabaseRequires(
            self.charm,
            relation_name=POSTGRESQL_RLEATION_NAME,
            database_name=getattr(self.state.postgresql_config, "database_name", ""),
            extra_user_roles=getattr(self.state.postgresql_config, "extra_user_roles", ""),
        )
        self.mysql = DatabaseRequires(
            self.charm,
            relation_name=MYSQL_RELATION_NAME,
            database_name=getattr(self.state.mysql_config, "database_name", ""),
            extra_user_roles=getattr(self.state.mysql_config, "extra_user_roles", ""),
        )
        self.mongodb = DatabaseRequires(
            self.charm,
            relation_name=MONGODB_RELATION_NAME,
            database_name=getattr(self.state.mysql_config, "database_name", ""),
            extra_user_roles=getattr(self.state.mysql_config, "extra_user_roles", ""),
        )
        self.spark = SparkServiceAccountRequirer(
            self.charm,
            relation_name=SPARK_RELATION_NAME,
            service_account=getattr(self.state.spark_config, "spark_service_account", ""),
            skip_creation=True,
        )
        for database in [self.postgresql, self.mysql, self.mongodb]:
            self.framework.observe(database.on.database_created, self._on_database_created)
            self.framework.observe(
                database.on.database_entity_created, self._on_database_entity_created
            )

        # opensearch
        self.framework.observe(self.opensearch.on.index_created, self._on_index_created)
        self.framework.observe(
            self.opensearch.on.index_entity_created, self._on_index_entity_created
        )
        self.framework.observe(
            self.charm.on[OPENSEARCH_RELATION_NAME].relation_broken, self._on_relation_broken
        )
        # kafka
        self.framework.observe(self.kafka.on.topic_created, self._on_topic_created)
        self.framework.observe(self.kafka.on.topic_entity_created, self._on_topic_entity_created)
        self.framework.observe(
            self.charm.on[KAFKA_RELATION_NAME].relation_broken, self._on_relation_broken
        )
        # spark
        self.framework.observe(
            self.spark.on.account_granted, self._on_spark_service_account_granted
        )
        self.framework.observe(self.spark.on.account_gone, self._on_spark_service_account_gone)
        self.framework.observe(self.spark.on.properties_changed, self._on_spark_properties_changed)

        # resource-dispatcher manifests
        self.secrets_manifests_wrapper = KubernetesManifestRequirerWrapper(
            charm=self.charm, relation_name=SECRETS_DISPATCHER_RELATION_NAME
        )
        self.service_accounts_manifests_wrapper = KubernetesManifestRequirerWrapper(
            charm=self.charm, relation_name=SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME
        )
        self.pod_defaults_manifests_wrapper = KubernetesManifestRequirerWrapper(
            charm=self.charm, relation_name=POD_DEFAULTS_DISPATCHER_RELATION_NAME
        )
        self.roles_manifests_wrapper = KubernetesManifestRequirerWrapper(
            charm=self.charm, relation_name=ROLES_DISPATCHER_RELATION_NAME
        )
        self.role_bindings_manifests_wrapper = KubernetesManifestRequirerWrapper(
            charm=self.charm, relation_name=ROLEBINDINGS_DISPATCHER_RELATION_NAME
        )

        # resource-dispatcher
        for relation_name in [
            SECRETS_DISPATCHER_RELATION_NAME,
            SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
            POD_DEFAULTS_DISPATCHER_RELATION_NAME,
            ROLES_DISPATCHER_RELATION_NAME,
            ROLEBINDINGS_DISPATCHER_RELATION_NAME,
        ]:
            self.framework.observe(
                self.charm.on[relation_name].relation_created,
                self._on_manifests_relation_change,
            )
            self.framework.observe(
                self.charm.on[relation_name].relation_changed,
                self._on_manifests_relation_change,
            )

    def _on_manifests_relation_change(self, _):
        """Event handler for when any of the manifests relations change."""
        # Only execute in the unit leader
        if not self.charm.unit.is_leader():
            return

        reconciled_manifests = ReconciledManifests()
        if self.charm.manifests_manager.is_manifests_provider_related:
            # Reconcile opensearch manifests
            opensearch_manifests = self.charm.opensearch_manager.generate_manifests()
            postgresql_manifests = self.charm.postgresql_manager.generate_manifests()
            mysql_manifests = self.charm.mysql_manager.generate_manifests()
            mongodb_manifests = self.charm.mongodb_manager.generate_manifests()
            kafka_manifests = self.charm.kafka_manager.generate_manifests()
            spark_manifests = self.charm.spark_manager.generate_manifests()
            reconciled_manifests = (
                reconciled_manifests
                + opensearch_manifests
                + postgresql_manifests
                + mysql_manifests
                + mongodb_manifests
                + kafka_manifests
                + spark_manifests
            )

        # TODO: Reconcile other Data Platform databases

        self.charm.manifests_manager.send_manifests(reconciled_manifests)

    def _on_database_created(self, event: DatabaseCreatedEvent) -> None:
        """Event triggered when a database is created for either mysql/postgresql/mongodb."""
        self.logger.debug(f"Database credentials are received: {event.username}")
        self._on_config_changed(event)

    def _on_database_entity_created(self, event: DatabaseEntityCreatedEvent) -> None:
        """Event triggered when a database entity is created for either mysql/postgresql/mongodb."""
        self.logger.debug(f"Database entity credentials are received: {event.entity_name}")
        self._on_config_changed(event)

    def _on_index_created(self, event: IndexCreatedEvent) -> None:
        """Event triggered when an index is created for this application."""
        self.logger.debug(f"OpenSearch credentials are received: {event.username}")
        self._on_config_changed(event)

    def _on_index_entity_created(self, event: IndexEntityCreatedEvent) -> None:
        """Event triggered when an entity is created for this application."""
        self.logger.debug(f"Entity credentials are received: {event.entity_name}")
        self._on_config_changed(event)

    def _on_topic_created(self, event: TopicCreatedEvent) -> None:
        """Event triggered when a topic is created for this application."""
        self.logger.debug(f"Topic is created and credentials received {event.username}")
        self._on_config_changed(event)

    def _on_topic_entity_created(self, event: TopicEntityCreatedEvent) -> None:
        """Event triggered when an entity is created for this application."""
        self.logger.debug(f"Entity credentials are received: {event.entity_name}")
        self._on_config_changed(event)

    def _on_spark_service_account_granted(self, event: ServiceAccountGrantedEvent) -> None:
        """Event triggered when a service account has been created and granted for this application."""
        self.logger.debug(f"Service account is gone: {event.service_account}")
        self._on_config_changed(event)

    def _on_spark_service_account_gone(self, event: ServiceAccountGoneEvent) -> None:
        """Event triggered when a service account has been released."""
        self.logger.debug("Service account is gone.")
        self._on_config_changed(event)

    def _on_spark_properties_changed(self, event: ServiceAccountPropertyChangedEvent) -> None:
        """Event triggered when the Spark properties for a service account has been changed."""
        self.logger.debug(
            f"Spark properties for the service account is changed: {event.service_account}"
        )
        self._on_config_changed(event)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle relation broken event."""
        pass

    def _on_config_changed(self, event: EventBase) -> None:
        """Event handler for configuration changed events."""
        # Only execute in the unit leader
        if not self.charm.unit.is_leader():
            return
        self.logger.debug(f"Config changed... Current configuration: {self.charm.config}")

        if self.state.opensearch_config and not self.charm.opensearch_manager.index_active:
            # route the config change to appropriate handler
            self.charm.opensearch_manager.update_relation_data()

        if self.state.postgresql_config and not self.charm.postgresql_manager.database_active:
            # route the config change to postgresql manager
            self.charm.postgresql_manager.update_relation_data()

        if self.state.mysql_config and not self.charm.mysql_manager.database_active:
            # route the config change to mysql manager
            self.charm.mysql_manager.update_relation_data()

        if self.state.mongodb_config and not self.charm.mongodb_manager.database_active:
            # route the config change to mongodb manager
            self.charm.mongodb_manager.update_relation_data()
        if self.state.kafka_config and not self.charm.kafka_manager.topic_active:
            # route the config change to kafka manager
            self.charm.kafka_manager.update_relation_data()

        if self.state.spark_config and not self.charm.spark_manager.service_account_active:
            # route the config change to spark manager
            self.charm.spark_manager.update_relation_data()

        # reconcile manifests
        self._on_manifests_relation_change(event)
