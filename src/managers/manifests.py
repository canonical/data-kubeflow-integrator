#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for kubernetes manifests generation tasks."""

import os

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from jinja2 import Environment, FileSystemLoader, Template
from pydantic import ValidationError


from core.config import ProfileConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from utils.logging import WithLogging

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_DIR)
TEMPLATE_DIR = os.path.join(SRC_DIR, "templates")


class KubernetesManifestsManager(ManagerStatusProtocol, WithLogging):
    """Manager for Kubernetes Manifests Generation and communication with 'resource-dispatcher'."""

    def __init__(self, state: GlobalState):
        self.name = "manifests"
        self.state = state
        self.env = Environment(
            loader=FileSystemLoader(TEMPLATE_DIR),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )

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

    @property
    def secret_k8s_template(self) -> Template:
        """Return template for generating kubernetes secrets."""
        return self.env.get_template("secret.yaml.tpl")

    @property
    def poddefault_k8s_template(self) -> Template:
        """Return template for generating kubernetes pod defaults."""
        return self.env.get_template("pod-defaults.yaml.tpl")
