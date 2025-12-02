#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import base64
import json
import logging
from pathlib import Path

import jubilant
import yaml
from helpers import (
    OPENSEARCH_MODEL_CONFIG,
    get_application_data,
    validate_k8s_poddefault,
    validate_k8s_secret,
)
from tenacity import Retrying, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
KUBEFLOW_INTEGRATOR_APP_NAME = "kubeflow-integrator"
OPENSEARCH_RELATION_NAME = "opensearch"

OPENSEARCH_CHARM = "opensearch"
OPENSEARCH_CHANNEL = "2/edge"
OPENSEARCH_PROFILE_CONFIG = "testing"
OPENSEARCH_APP_NAME = "opensearch"

SELF_SIGNED_CERTIFICATES_CHARM = "self-signed-certificates"
SELF_SIGNED_CERTIFICATES_CHANNEL = "1/stable"
SELF_SIGNED_CERTIFICATES_APP_NAME = "self-signed-certificates"


METACONTROLLER_CHARM = "metacontroller-operator"
ADMISSION_WEBHOOK_CHARM = "admission-webhook"
RESOURCE_DISPATCHER = "resource-dispatcher"
RESOURCE_DISPATCHER_APP_NAME = "resource-dispatcher"
RESOURCE_DISPATCHER_CHANNEL = "latest/edge"


def test_integrate_kubeflow_with_resource_dispatcher(
    juju: jubilant.Juju, kubeflow_integrator: str
):
    """Tesing the integration between kubeflow-integrator and resource disptacher.

    Test deploying the kubeflow-integrator charm and resource dispatcher, integrate
    them and validate all active.
    """
    # Install resource dispatcher and its dependencies
    juju.deploy(METACONTROLLER_CHARM, trust=True)
    juju.deploy(ADMISSION_WEBHOOK_CHARM, trust=True)
    juju.deploy(
        RESOURCE_DISPATCHER,
        trust=True,
        channel=RESOURCE_DISPATCHER_CHANNEL,
        app=RESOURCE_DISPATCHER_APP_NAME,
    )
    juju.deploy(
        kubeflow_integrator,
        app=KUBEFLOW_INTEGRATOR_APP_NAME,
        config={"profile": "profile-name"},
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR_APP_NAME}:secrets", f"{RESOURCE_DISPATCHER_APP_NAME}:secrets"
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR_APP_NAME}:pod-defaults",
        f"{RESOURCE_DISPATCHER_APP_NAME}:pod-defaults",
    )

    # Wait for charms to be active
    status = juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )
    logger.info(status)

    logger.info("Checking secrets and pod defaults relation data")
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(3), reraise=True):
        with attempt:
            # Check application data
            secrets_rel_data = list(
                get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "secrets").items()
            )[0]
            assert secrets_rel_data[1] == {}
            assert "kubernetes_manifests" not in secrets_rel_data[1]
            poddefaults_rel_data = list(
                get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "pod-defaults").items()
            )[0]
            assert poddefaults_rel_data[1] == {}
            assert "kubernetes_manifests" not in poddefaults_rel_data[1]


def test_manifests_generation_with_opensearch(
    juju: jubilant.Juju, juju_vm: jubilant.Juju, vm_controller: str, k8s_controller: str
):
    """Tesing Manifests Generation when related to OpenSearch.

    Deploy opensearch, integrate it with kubeflow-integrator and make sure that
    manifests are generated in the relation data.
    """
    juju.config(KUBEFLOW_INTEGRATOR_APP_NAME, {"opensearch-index-name": "index-name"})

    # Switch to VM controller
    juju.cli("switch", vm_controller, include_model=False)

    logger.info("Configure model with opensearch required config")
    juju_vm.model_config(OPENSEARCH_MODEL_CONFIG)

    # Deploy opensearch and self-signed-certificates
    logger.info("Deploying OpenSearch charm")
    juju_vm.deploy(
        OPENSEARCH_CHARM,
        app=OPENSEARCH_APP_NAME,
        channel=OPENSEARCH_CHANNEL,
        config={"profile": OPENSEARCH_PROFILE_CONFIG},
    )

    logger.info("Deploying self-signed-certificates charm")
    juju_vm.deploy(
        SELF_SIGNED_CERTIFICATES_CHARM,
        app=SELF_SIGNED_CERTIFICATES_APP_NAME,
        channel=SELF_SIGNED_CERTIFICATES_CHANNEL,
    )
    logger.info("Integrate opensearch with self-signed-certificates")
    # Integrate opensearch with self-signed-certificates
    juju_vm.integrate(OPENSEARCH_APP_NAME, SELF_SIGNED_CERTIFICATES_APP_NAME)

    logger.info("Waiting for apps to settle in...")
    juju_vm.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status),
        delay=5,
        timeout=1800,
    )

    logger.info("Offering the opensearch client relation")
    juju_vm.offer(OPENSEARCH_APP_NAME, "opensearch-client")

    # Switch back to k8s controller
    juju_vm.cli("switch", k8s_controller, include_model=False)

    juju.consume(f"{juju_vm.model}.{OPENSEARCH_APP_NAME}", controller=vm_controller)

    logger.info("Integrate opensearch with kubeflow-integrator")
    juju.integrate(
        f"{OPENSEARCH_APP_NAME}:opensearch-client",
        f"{KUBEFLOW_INTEGRATOR_APP_NAME}:{OPENSEARCH_RELATION_NAME}",
    )

    logger.info("Waiting for kubeflow-integrator to be active")
    juju.wait(
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR_APP_NAME)
        and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR_APP_NAME),
        delay=5,
        timeout=600,
    )

    # Check application data
    secrets_rel_data = list(
        get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "secrets").items()
    )[0]
    assert secrets_rel_data[1] != {}
    poddefaults_rel_data = list(
        get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "pod-defaults").items()
    )[0]
    assert poddefaults_rel_data[1] != {}

    assert "kubernetes_manifests" in secrets_rel_data[1]
    secrets_k8s_manifests = json.loads(secrets_rel_data[1]["kubernetes_manifests"])
    for secret_manifest in secrets_k8s_manifests:
        secret = validate_k8s_secret(secret_manifest)
        if secret.metadata.name == "opensearch-secret":
            assert secret.data["OPENSEARCH_INDEX"] == base64.b64encode(b"index-name").decode()

    assert "kubernetes_manifests" in poddefaults_rel_data[1]
    poddefaults_k8s_manifests = json.loads(poddefaults_rel_data[1]["kubernetes_manifests"])
    for poddefault_manifest in poddefaults_k8s_manifests:
        validate_k8s_poddefault(poddefault_manifest)
