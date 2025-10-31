#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for kubernetes manifests generation tasks."""

import os

from charms.resource_dispatcher.v0.kubernetes_manifests import (
    KubernetesManifest,  # type: ignore[import-untyped]
)
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from jinja2 import Environment, FileSystemLoader, Template
from pydantic import ValidationError

from constants import (
    K8S_DATABASE_SECRET_NAME,
    K8S_DATABASE_TLS_CERT_PATH,
    K8S_DATABASE_TLS_SECRET_NAME,
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
)
from core.config import ProfileConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from utils.helpers_manifests import (
    generate_poddefault_manifest,
    generate_secret_manifest,
    generate_tls_secret_manifest,
)
from utils.k8s_models import (
    ReconciledManifests,
)
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

    def reconcile_database_manifests(
        self, creds: dict[str, str], database_name: str
    ) -> ReconciledManifests:
        """Generate manifests for OpenSearch/Mysql/Postgresql/Mongodb databases."""
        if not self.state.profile_config:
            self.logger.warning("No specified profile, skipping manifests generation")
            return ReconciledManifests()

        database_secret: KubernetesManifest | None = None
        tls_secret: KubernetesManifest | None = None
        secrets_manifests: list[KubernetesManifest] = []
        poddefault_manifests: list[KubernetesManifest] = []

        # remove data field from the credentials
        if "data" in creds:
            creds.pop("data", None)

        # generate secrets manifests
        if self.is_k8s_secrets_manifests_related:
            secret_data = creds.copy()
            tls_secret = generate_tls_secret_manifest(
                self.secret_k8s_template,
                self.state.profile_config.profile,
                creds,
                database_name,
            )
            if tls_secret:
                secrets_manifests.append(tls_secret)
                # Add a path to the mounted tls
                secret_data["ca_cert_path"] = K8S_DATABASE_TLS_CERT_PATH[database_name]
                # Remove "tls-ca" from creds
                secret_data.pop("tls-ca")

            database_secret = generate_secret_manifest(
                self.secret_k8s_template,
                self.state.profile_config.profile,
                secret_data,
                database_name,
            )
            secrets_manifests.append(database_secret)
        # generate pod defaults
        if self.is_k8s_poddefaults_manifests_related:
            # If a tls-secret was generated, we include the cert path in pod default
            if tls_secret:
                creds["ca_cert_path"] = K8S_DATABASE_TLS_CERT_PATH[database_name]
                creds.pop("tls-ca")

            poddefault = generate_poddefault_manifest(
                self.poddefault_k8s_template,
                self.state.profile_config.profile,
                creds,
                database_name,
                from_secret=K8S_DATABASE_SECRET_NAME[database_name] if database_secret else None,
                tls_secret=K8S_DATABASE_TLS_SECRET_NAME[database_name] if tls_secret else None,
            )
            poddefault_manifests.append(poddefault)
        return ReconciledManifests(secrets=secrets_manifests, poddefaults=poddefault_manifests)

    def send_manifests(self, reconciled_manifests: ReconciledManifests):
        """Send k8s manifests to the manifests provider."""
        if len(reconciled_manifests.secrets):
            self.manifests_secret_wrapper.send_data(reconciled_manifests.secrets)
        if len(reconciled_manifests.poddefaults):
            self.manifests_poddefault_wrapper.send_data(reconciled_manifests.poddefaults)
        if len(reconciled_manifests.serviceaccounts):
            self.manifests_service_account_wrapper.send_data(reconciled_manifests.serviceaccounts)

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        status_list = []
        if scope == "app":
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
        else:
            return [CharmStatuses.ACTIVE_IDLE.value]

    @property
    def manifests_secret_wrapper(self):
        """Return the Manifests Secret Wrapper."""
        return self.state.charm.general_events.secrets_manifests_wrapper

    @property
    def manifests_poddefault_wrapper(self):
        """Return the Manifests PodDefault Wrapper."""
        return self.state.charm.general_events.pod_defaults_manifests_wrapper

    @property
    def manifests_service_account_wrapper(self):
        """Retuyrn the Manifests Service Account Wrapper."""
        return self.state.charm.general_events.service_accounts_manifests_wrapper

    @property
    def is_manifests_provider_related(self):
        """Is the charm related to any manifests relation provider."""
        return any(
            [
                self.is_k8s_poddefaults_manifests_related,
                self.is_k8s_secrets_manifests_related,
                self.is_k8s_service_accounts_manifests_related,
            ]
        )

    @property
    def is_k8s_secrets_manifests_related(self) -> bool:
        """Is the charm related to a secrets manifests relation."""
        return bool(self.state.charm.model.relations.get(SECRETS_DISPATCHER_RELATION_NAME))

    @property
    def is_k8s_poddefaults_manifests_related(self) -> bool:
        """Is the charm related to a secrets manifests relation."""
        return bool(self.state.charm.model.relations.get(POD_DEFAULTS_DISPATCHER_RELATION_NAME))

    @property
    def is_k8s_service_accounts_manifests_related(self) -> bool:
        """Is the charm related to a secrets manifests relation."""
        return bool(
            self.state.charm.model.relations.get(SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME)
        )

    @property
    def secret_k8s_template(self) -> Template:
        """Return template for generating kubernetes secrets."""
        return self.env.get_template("secret.yaml.tpl")

    @property
    def poddefault_k8s_template(self) -> Template:
        """Return template for generating kubernetes pod defaults."""
        return self.env.get_template("pod-defaults.yaml.tpl")
