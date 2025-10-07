#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for General Kubeflow-integrator charm events."""

from __future__ import annotations

from typing import TYPE_CHECKING
from managers.manifests import ReconciledManifests
import ops
from ops.charm import ConfigChangedEvent
from ops import Object

from utils.logging import WithLogging

from constants import (
    OPENSEARCH,
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
    OPENSEARCH,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
)
from charms.resource_dispatcher.v0.kubernetes_manifests import KubernetesManifestRequirerWrapper
from charms.data_platform_libs.v0.data_interfaces import (
    IndexCreatedEvent,
    IndexEntityCreatedEvent,
    OpenSearchRequires,
)
from ops import (
    RelationBrokenEvent,
)
from core.state import GlobalState

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class GeneralEventsHandler(Object, WithLogging):
    """Class implementing Kubeflow-integrator events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="general")

        self.charm = charm
        self.state = state

        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)
        self.framework.observe(self.charm.on.secret_changed, self._on_secret_changed)

        # 3rd database
        self.opensearch = OpenSearchRequires(
            self.charm,
            OPENSEARCH,
            getattr(self.state.opensearch_config, "index_name", ""),
            extra_user_roles=getattr(self.state.opensearch_config, "extra_user_roles", ""),
        )
        # opensearch
        self.framework.observe(self.opensearch.on.index_created, self._on_index_created)
        self.framework.observe(self.opensearch.on.index_entity_created, self._on_entity_created)
        self.framework.observe(self.charm.on[OPENSEARCH].relation_broken, self._on_relation_broken)

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

        # resource-dispatcher
        self.framework.observe(
            self.charm.on[SECRETS_DISPATCHER_RELATION_NAME].relation_created,
            self._on_manifests_relation_change,
        )
        self.framework.observe(
            self.charm.on[SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME].relation_created,
            self._on_manifests_relation_change,
        )
        self.framework.observe(
            self.charm.on[POD_DEFAULTS_DISPATCHER_RELATION_NAME].relation_created,
            self._on_manifests_relation_change,
        )

    def _on_manifests_relation_change(self, _):
        """Event handler for when any of the manifests relations change"""
        reconciled_manifests = ReconciledManifests()
        if self.charm.manifests_manager.is_manifests_provider_related:
            # Reconcile opensearch manifests
            opensearch_manifests = self.charm.opensearch_manager.generate_manifests()
            reconciled_manifests = reconciled_manifests + opensearch_manifests
        # TODO: Reconcile other Data Platform databases

        self.charm.manifests_manager.send_manifests(reconciled_manifests)

    def _on_index_created(self, event: IndexCreatedEvent) -> None:
        """Event triggered when an index is created for this application."""
        self.logger.debug(f"OpenSearch credentials are received: {event.username}")
        self._on_config_changed(event)

    def _on_entity_created(self, event: IndexEntityCreatedEvent) -> None:
        """Event triggered when an entity is created for this application."""
        self.logger.debug(f"Entity credentials are received: {event.entity_name}")
        self._on_config_changed(event)

    def _on_relation_broken(self, event: RelationBrokenEvent) -> None:
        """Handle relation broken event."""
        pass

    def _on_config_changed(self, event: ConfigChangedEvent) -> None:  # noqa: C901
        """Event handler for configuration changed events."""
        # Only execute in the unit leader
        if not self.charm.unit.is_leader():
            return
        self.logger.debug(f"Config changed... Current configuration: {self.charm.config}")

        if self.state.opensearch_config and not self.charm.opensearch_manager.index_active:
            # route the config change to appropriate handler
            self._on_opensearch_confg_changed()

        # TODO: Add handlers for other DataPlatform databases

        # reconcile manifests
        self._on_manifests_relation_change(event)

    def _on_opensearch_confg_changed(self) -> None:
        """Handle on config changed for opensearch."""
        self.charm.opensearch_manager.update_relation_data()

    def _on_secret_changed(self, event: ops.SecretChangedEvent) -> None:
        """Handle the secret changed event.

        When a secret is changed, it is first checked that whether this particular secret
        is used in the charm's config. If yes, the secret is to be updated in the relation
        databag.
        """
        # Only execute in the unit leader
        if not self.charm.unit.is_leader():
            return
