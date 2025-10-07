#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for opensearch related tasks."""

from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from core.config import OpenSearchConfig
from core.state import GlobalState
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from core.statuses import CharmStatuses, ConfigStatuses
from pydantic import ValidationError
from utils.k8s_models import ReconciledManifests
from utils.logging import WithLogging


class OpenSearchManager(ManagerStatusProtocol, WithLogging):
    """Manager for Opensearch relation."""

    def __init__(self, state: GlobalState):
        self.name = "opensearch"
        self.state = state

    def update_relation_data(self) -> None:
        """Update opensearch relation data with latest config."""
        opensearch_config = self.state.opensearch_config
        if opensearch_config:
            relation_data = {
                "index": opensearch_config.index_name if opensearch_config else "",
                "extra-user-roles": opensearch_config.extra_user_roles or "",
            }
            for rel in self.opensearch_requirer.relations:
                self.opensearch_requirer.update_relation_data(rel.id, relation_data)

    def generate_manifests(self) -> ReconciledManifests:
        """Generate kubernetes manifesets for the current credentials"""
        if self.is_opensearch_related and self.index_active:
            # Fetch credentials
            opensearch_creds = list(self.opensearch_requirer.fetch_relation_data().values())[0]
            return self.state.charm.manifests_manager.reconcile_opensearch_manifests(
                opensearch_creds
            )
        return ReconciledManifests()

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        status_list = []

        opensearch_config = None

        try:
            opensearch_config = OpenSearchConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.warning(str(err))

            # If opensearch is related
            if len(self.opensearch_requirer.relations) > 0:
                missing = [
                    str(error["loc"][0]) for error in err.errors() if error["type"] == "missing"
                ]
                invalid = [
                    str(error["loc"][0]) for error in err.errors() if error["type"] != "missing"
                ]

                if missing:
                    status_list.append(ConfigStatuses.missing_config_parameters(fields=missing))
                if invalid:
                    status_list.append(ConfigStatuses.invalid_config_parameters(fields=invalid))

        if opensearch_config and not self.is_opensearch_related:
            # Block the charm since we need the integration with opensearch
            status_list.append(CharmStatuses.missing_integration_with_opensearch())

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]

    @property
    def opensearch_requirer(self) -> OpenSearchRequires:
        """Return the opensearchRequires instance from event handlers"""
        return self.state.charm.general_events.opensearch

    @property
    def index_active(self) -> str | None:
        """Return the created and configured opensearch index"""
        if (
            relation := self.opensearch_requirer.relations[0]
            if len(self.opensearch_requirer.relations)
            else None
        ):
            return self.opensearch_requirer.fetch_relation_field(relation.id, "index")

    @property
    def is_opensearch_related(self) -> bool:
        """Check if we have a relation with OpenSearch"""
        for relation in self.opensearch_requirer.relations:
            data = self.opensearch_requirer.fetch_relation_data(
                [relation.id], ["username", "password"]
            ).get(relation.id, {})
            if all(data.get(key) for key in ("username", "password")):
                return True
        return False
