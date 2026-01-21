#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for Kubeflow profiles."""

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from pydantic import ValidationError

from core.config import ProfileConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from utils.logging import WithLogging


class KubeflowProfileManager(ManagerStatusProtocol, WithLogging):
    """Manager for Kubeflow profile."""

    def __init__(self, state: GlobalState):
        self.name = "profile"
        self.state = state

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        status_list = []
        try:
            ProfileConfig(**self.state.charm.config)
        except ValidationError as err:
            self.logger.error(f"A validation error occurred {err}")
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

        return status_list or [CharmStatuses.ACTIVE_IDLE.value]
