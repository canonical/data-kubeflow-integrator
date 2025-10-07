#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Object representing the state of KubeflowIntegrator Charm."""

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.data_interfaces import (
    DataPeerData,
    DataPeerUnitData,
)
from data_platform_helpers.advanced_statuses.protocol import StatusesState, StatusesStateProtocol
from ops import Object, Relation

from constants import (
    PEER_RELATION,
    STATUS_PEERS_RELATION_NAME,
)
from core.config import OpenSearchConfig, ProfileConfig
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
        self.statuses = StatusesState(self, STATUS_PEERS_RELATION_NAME)

    @property
    def peer_relation(self) -> Relation | None:
        """Get the cluster peer relation."""
        return self.model.get_relation(PEER_RELATION)

    @property
    def opensearch_config(self) -> OpenSearchConfig | None:
        """Return information regarding opensearch config options."""
        try:
            validated_config = OpenSearchConfig(**self.config)
        except Exception:
            return None

        return validated_config

    @property
    def profile_config(self) -> ProfileConfig | None:
        """Return information regarding the profile config option."""
        try:
            validated_config = ProfileConfig(**self.config)
        except Exception:
            return None

        return validated_config
