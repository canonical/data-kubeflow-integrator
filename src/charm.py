#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

import ops
from data_platform_helpers.advanced_statuses.handler import StatusHandler

from core.state import GlobalState
from events.general import GeneralEventsHandler
from managers.manifests import KubernetesManifestsManager
from managers.opensearch import OpenSearchManager

logger = logging.getLogger(__name__)


class KubeflowIntegratorCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, *args):
        super().__init__(*args)

        # Context
        self.state = GlobalState(self)

        # Managers
        self.opensearch_manager = OpenSearchManager(self.state)
        self.manifests_manager = KubernetesManifestsManager(self.state)

        self.status = StatusHandler(  # priority order
            self, self.manifests_manager, self.opensearch_manager
        )

        # Event Handlers
        self.general_events = GeneralEventsHandler(self, self.state)


if __name__ == "__main__":  # pragma: nocover
    ops.main(KubeflowIntegratorCharm)
