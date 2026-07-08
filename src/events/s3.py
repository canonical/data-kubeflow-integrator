#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Handler for S3 related events."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
)
from ops import Object

from core.state import GlobalState
from utils.logging import WithLogging

if TYPE_CHECKING:
    from charm import KubeflowIntegratorCharm


class S3EventsHandler(Object, WithLogging):
    """Class implementing S3 events handling."""

    def __init__(self, charm: KubeflowIntegratorCharm, state: GlobalState):
        super().__init__(charm, key="s3")
        self.charm = charm
        self.state = state

        self.framework.observe(
            self.state.s3_requirer.on.credentials_changed, self._on_s3_credentials_changed
        )
        self.framework.observe(
            self.state.s3_requirer.on.credentials_gone, self._on_s3_credentials_gone
        )

    def _on_s3_credentials_changed(self, event: CredentialsChangedEvent) -> None:
        """Event triggered when S3 credentials are made available for this application."""
        self.logger.debug("S3 credentials are available.")
        self.charm.general_events._on_config_changed(event)

    def _on_s3_credentials_gone(self, event: CredentialsGoneEvent) -> None:
        """Event triggered when S3 credentials are removed."""
        self.logger.debug("S3 credentials are gone.")
        self.charm.general_events._on_config_changed(event)
