#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for K8s resource manifest related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.resource_dispatcher.v0.kubernetes_manifests import (
    KubernetesManifestRequirerWrapper,  # type: ignore
)
from ops import Object

from constants import (
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
    ROLEBINDINGS_DISPATCHER_RELATION_NAME,
    ROLES_DISPATCHER_RELATION_NAME,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
)
from core.state import GlobalState
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class ManifestEventsHandler(Object, WithLogging):
    """Class implementing K8s resource manifest events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="general")

        self.charm = charm
        self.state = state

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
        if self.state.is_manifests_provider_related():
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

        self.send_manifests(reconciled_manifests)

    def send_manifests(self, reconciled_manifests: ReconciledManifests):
        """Send k8s manifests to the manifests provider."""
        self.secrets_manifests_wrapper.send_data(reconciled_manifests.secrets)
        self.pod_defaults_manifests_wrapper.send_data(reconciled_manifests.poddefaults)
        self.service_accounts_manifests_wrapper.send_data(reconciled_manifests.serviceaccounts)
        self.roles_manifests_wrapper.send_data(reconciled_manifests.roles)
        self.role_bindings_manifests_wrapper.send_data(reconciled_manifests.role_bindings)
