#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for kubernetes manifests generation tasks."""

import os

from jinja2 import Environment, FileSystemLoader, Template

from core.state import GlobalState
from utils.logging import WithLogging

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.dirname(CURRENT_DIR)
TEMPLATE_DIR = os.path.join(SRC_DIR, "templates")


class KubernetesManifestsManager(WithLogging):
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

    @property
    def secret_k8s_template(self) -> Template:
        """Return template for generating kubernetes secrets."""
        return self.env.get_template("secret.yaml.tpl")

    @property
    def poddefault_k8s_template(self) -> Template:
        """Return template for generating kubernetes pod defaults."""
        return self.env.get_template("pod-defaults.yaml.tpl")

    @property
    def minio_artifact_secret_template(self) -> Template:
        """Return template for the mlpipeline-minio-artifact Secret."""
        return self.env.get_template("minio-artifact-secret.yaml.tpl")

    @property
    def artifact_repositories_template(self) -> Template:
        """Return template for the argo artifact-repositories ConfigMap."""
        return self.env.get_template("artifact-repositories.yaml.tpl")

    @property
    def kfp_launcher_template(self) -> Template:
        """Return template for the kfp-launcher ConfigMap."""
        return self.env.get_template("kfp-launcher.yaml.tpl")
