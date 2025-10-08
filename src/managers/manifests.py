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
    K8S_OPENSEARCH_PODDEFAULT_DESC,
    K8S_OPENSEARCH_PODDEFAULT_NAME,
    K8S_OPENSEARCH_PODDEFAULT_SELECTOR_LABEL,
    K8S_OPENSEARCH_SECRET_NAME,
    POD_DEFAULTS_DISPATCHER_RELATION_NAME,
    SECRETS_DISPATCHER_RELATION_NAME,
    SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME,
)
from core.config import ProfileConfig
from core.state import GlobalState
from core.statuses import CharmStatuses, ConfigStatuses
from utils.k8s_models import (
    EnvVarFromSecret,
    K8sPodDefaultManifestInfo,
    K8sSecretManifestInfo,
    PodDefaultEnvVar,
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

    def reconcile_opensearch_manifests(self, creds: dict[str, str]) -> ReconciledManifests:
        """Generate opensearch manifests using credentials and send to resource-dispatcher."""
        if not self.state.profile_config:
            self.logger.warning("No specified profile, skipping manifests generation")
            return ReconciledManifests()

        secrets_manifests: list[KubernetesManifest] = []
        poddefaults_manifests: list[KubernetesManifest] = []
        service_accounts_manifests: list[KubernetesManifest] = []

        # remove data field from the credentials dict
        if "data" in creds:
            creds.pop("data", None)

        if self.is_k8s_secrets_manifests_related:
            # clean tls certificate field
            creds["tls-ca"] = creds["tls-ca"].replace("\n", "")

            k8s_secret_info = K8sSecretManifestInfo(
                name=K8S_OPENSEARCH_SECRET_NAME,
                namespace=None
                if self.state.profile_config.profile == "*"
                else self.state.profile_config.profile,
                data=creds,
                labels=None,
            )
            # generate using jinja
            rendered = self.secret_k8s_template.render(secret=k8s_secret_info)
            secrets_manifests.append(KubernetesManifest(rendered))

        if self.is_k8s_poddefaults_manifests_related:
            if len(secrets_manifests):
                poddefault_env_vars = [
                    PodDefaultEnvVar(
                        name=key,
                        secret=EnvVarFromSecret(
                            secret_name=K8S_OPENSEARCH_SECRET_NAME, secret_key=key
                        ),
                    )
                    for key, _ in creds.items()
                ]
            else:
                poddefault_env_vars = [
                    PodDefaultEnvVar(name=key, value=value) for key, value in creds.items()
                ]
            k8s_poddefault_info = K8sPodDefaultManifestInfo(
                name=K8S_OPENSEARCH_PODDEFAULT_NAME,
                namespace=None
                if self.state.profile_config.profile == "*"
                else self.state.profile_config.profile,
                desc=K8S_OPENSEARCH_PODDEFAULT_DESC,
                selector_name=K8S_OPENSEARCH_PODDEFAULT_SELECTOR_LABEL,
                env_vars=poddefault_env_vars,
            )
            rendered = self.poddefault_k8s_template.render(pod_default=k8s_poddefault_info)
            poddefaults_manifests.append(KubernetesManifest(rendered))

        return ReconciledManifests(
            secrets=secrets_manifests,
            poddefaults=poddefaults_manifests,
            serviceaccounts=service_accounts_manifests,
        )

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

        try:
            ProfileConfig(**self.state.charm.config)
        except ValidationError as err:
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
