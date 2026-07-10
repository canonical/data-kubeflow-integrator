#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for KFP related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from object_storage import (
    StorageConnectionInfoChangedEvent,
    StorageConnectionInfoGoneEvent,
)
from ops import Object

from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class KFPEventsHandler(Object, WithLogging):
    """Class implementing KFP events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="kfp")
        self.charm = charm
        self.state = state

        self.framework.observe(
            self.state.kfp_requirer.on.storage_connection_info_changed,
            self._on_kfp_credentials_changed,
        )
        self.framework.observe(
            self.state.kfp_requirer.on.storage_connection_info_gone,
            self._on_kfp_credentials_gone,
        )

    def _on_kfp_credentials_changed(self, event: StorageConnectionInfoChangedEvent) -> None:
        """Event triggered when KFP credentials are made available for this application."""
        self.logger.debug("KFP credentials are available.")
        self.charm.general_events._on_config_changed(event)

    def _on_kfp_credentials_gone(self, event: StorageConnectionInfoGoneEvent) -> None:
        """Event triggered when KFP credentials are removed."""
        self.logger.debug("KFP credentials are gone.")
        self.charm.general_events._on_config_changed(event)
