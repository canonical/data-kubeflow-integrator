#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for Spark related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    ServiceAccountGoneEvent,
    ServiceAccountGrantedEvent,
    ServiceAccountPropertyChangedEvent,
    SparkServiceAccountRequirerEventHandlers,
)
from ops import Object

from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class SparkEventsHandler(Object, WithLogging):
    """Class implementing Spark events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="spark")
        self.charm = charm
        self.state = state

        self.spark = SparkServiceAccountRequirerEventHandlers(
            self.charm, self.state.spark_requirer
        )

        self.framework.observe(
            self.spark.on.account_granted, self._on_spark_service_account_granted
        )
        self.framework.observe(self.spark.on.account_gone, self._on_spark_service_account_gone)
        self.framework.observe(self.spark.on.properties_changed, self._on_spark_properties_changed)

    def _on_spark_service_account_granted(self, event: ServiceAccountGrantedEvent) -> None:
        """Event triggered when a service account has been created and granted for this application."""
        self.logger.debug(f"Spark service account is granted: {event.service_account}")
        self.charm.general_events._on_config_changed(event)

    def _on_spark_service_account_gone(self, event: ServiceAccountGoneEvent) -> None:
        """Event triggered when a service account has been released."""
        self.logger.debug("Spark service account is gone.")
        self.charm.general_events._on_config_changed(event)

    def _on_spark_properties_changed(self, event: ServiceAccountPropertyChangedEvent) -> None:
        """Event triggered when the Spark properties for a service account has been changed."""
        self.logger.debug(
            f"Spark properties for the service account is changed: {event.service_account}"
        )
        self.charm.general_events._on_config_changed(event)

    def update_relation_data(self) -> None:
        """Update spark relation data with latest config."""
        spark_config = self.state.spark_config
        profile_config = self.state.profile_config
        if spark_config and profile_config:
            username = spark_config.spark_service_account
            profile = profile_config.profile
            namespace = profile if profile != "*" else self.charm.model.name
            relation_data = {
                "service-account": f"{namespace}:{username}",
                "skip-creation": "true",
            }
            for rel in self.state.spark_requirer.relations:
                self.state.spark_requirer.update_relation_data(rel.id, relation_data)
