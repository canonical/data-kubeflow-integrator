#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import logging
from pathlib import Path
import time

import jubilant
import yaml
import json

from helpers import get_application_data, validate_k8s_poddefault, validate_k8s_secret
from tenacity import Retrying, sleep, stop_after_attempt, wait_fixed

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
SELF_SIGNED_CERTIFICATES_CHANNEL = "latest/stable"
SELF_SIGNED_CERTIFICATES_APP_NAME = "self-signed-certificates"


METACONTROLLER_CHARM = "metacontroller-operator"
ADMISSION_WEBHOOK_CHARM = "admission-webhook"
RESOURCE_DISPATCHER = "resource-dispatcher"
RESOURCE_DISPATCHER_APP_NAME = "resource-dispatcher"
RESOURCE_DISPATCHER_CHANNEL = "latest/edge"


def test_integrate_with_resource_dispatcher(
    juju: jubilant.Juju, microk8s_model: str, kubeflow_integrator: str
):
    """
    Test deploying the kuebflow-integrator charm and resource dispatcher, integrate them
    and validate all active.
    """
    # save the temp_model
    temp_model = juju.model
    # Switch to the k8s model
    juju.model = microk8s_model
    # Install resource dispatcher and its dependencies
    juju.deploy(METACONTROLLER_CHARM, trust=True)
    juju.deploy(ADMISSION_WEBHOOK_CHARM, trust=True)
    juju.deploy(
        RESOURCE_DISPATCHER,
        trust=True,
        channel=RESOURCE_DISPATCHER_CHANNEL,
        app=RESOURCE_DISPATCHER_APP_NAME,
    )

    # Wait for charms to be active
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # Offer interfaces
    logger.info("Offering resource dispatcher interfaces")
    juju.offer(RESOURCE_DISPATCHER_APP_NAME, name="secrets-dispatcher", endpoint="secrets")
    juju.offer(
        RESOURCE_DISPATCHER_APP_NAME, name="poddefaults-dispatcher", endpoint="pod-defaults"
    )

    # Switch to the lxd model
    logger.info("Switching to kubeflow-integrator model")
    juju.model = temp_model

    juju.deploy(
        kubeflow_integrator,
        app=KUBEFLOW_INTEGRATOR_APP_NAME,
        config={"profile": "profile-name"},
    )

    # Consume offers
    logger.info("Consuming offers")
    juju.consume(f"{microk8s_model}.secrets-dispatcher")
    juju.consume(f"{microk8s_model}.poddefaults-dispatcher")

    logger.info("Waiting for all units to become active")
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # Integrate kubeflow-integrator with resource-dispatcher
    logger.info("Integrate kubeflow-integrator with resource-dispatcher")
    juju.integrate(f"{KUBEFLOW_INTEGRATOR_APP_NAME}:secrets", "secrets-dispatcher")
    juju.integrate(f"{KUBEFLOW_INTEGRATOR_APP_NAME}:pod-defaults", "poddefaults-dispatcher")

    # Wait for the relatio data to be created

    # Switch to k8s model
    juju.model = microk8s_model

    logger.info("Checking secrets and pod defaults relation data")

    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(3), reraise=True):
        with attempt:
            # Check application data
            secrets_rel_data = list(
                get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "secrets").items()
            )[0]
            assert secrets_rel_data[1] == {}
            poddefaults_rel_data = list(
                get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "pod-defaults").items()
            )[0]
            assert poddefaults_rel_data[1] == {}

    juju.model = temp_model


def test_manifests_generation_with_opensearch(juju: jubilant.Juju, microk8s_model: str):
    """
    Deploy opensearch, integrate it with kubeflow-integrator and make sure that
    manifests are generated in the relation data.
    """
    temp_model = juju.model
    # Deploy opensearch and self-signed-certificates
    logger.info("Deploying OpenSearch charm")
    juju.config(KUBEFLOW_INTEGRATOR_APP_NAME, {"opensearch-index-name": "index-name"})

    juju.deploy(
        OPENSEARCH_CHARM,
        app=OPENSEARCH_APP_NAME,
        channel=OPENSEARCH_CHANNEL,
        config={"profile": OPENSEARCH_PROFILE_CONFIG},
    )

    logger.info("Deploying self-signed-certificates charm")
    juju.deploy(
        SELF_SIGNED_CERTIFICATES_CHARM,
        app=SELF_SIGNED_CERTIFICATES_APP_NAME,
        channel=SELF_SIGNED_CERTIFICATES_CHANNEL,
    )
    logger.info("Integrate opensearch with self-signed-certificates")
    # Integrate opensearch with self-signed-certificates
    juju.integrate(OPENSEARCH_APP_NAME, SELF_SIGNED_CERTIFICATES_APP_NAME)

    logger.info("Waiting for opensearch to be active")
    juju.wait(
        lambda status: jubilant.all_active(status, OPENSEARCH_APP_NAME)
        and jubilant.all_agents_idle(status, OPENSEARCH_APP_NAME),
        delay=5,
    )

    logger.info("Integrate opensearch with kubeflow-integrator")
    juju.integrate(OPENSEARCH_APP_NAME, KUBEFLOW_INTEGRATOR_APP_NAME)

    logger.info("Waiting for kubeflow-integrator to be active")
    juju.wait(
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR_APP_NAME)
        and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR_APP_NAME),
        delay=5,
    )

    # Switch to k8s model
    juju.model = microk8s_model

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
        validate_k8s_secret(secret_manifest, keys_values_to_check={"index": "index-name"})

    assert "kubernetes_manifests" in poddefaults_rel_data[1]
    poddefaults_k8s_manifests = json.loads(poddefaults_rel_data[1]["kubernetes_manifests"])
    for poddefault_manifest in poddefaults_k8s_manifests:
        validate_k8s_poddefault(poddefault_manifest)

    juju.model = temp_model
