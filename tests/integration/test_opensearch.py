#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml
from helpers import get_application_data

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


def test_deploy_and_config_opensearch(juju: jubilant.Juju, kubeflow_integrator: str):
    """Deploy Kubeflow Integrator, configure opensearch-index-name. The charm should be in a blocked status waiting to be integrated with opensearch."""
    logger.info("Deploying Kubeflow Integrator charm")
    # When:
    juju.deploy(
        kubeflow_integrator, app=KUBEFLOW_INTEGRATOR_APP_NAME, config={"profile": "profile-name"}
    )

    # Assert:
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    logger.info("Configure `opensearch-index-name` config option")
    # When:

    juju.config(KUBEFLOW_INTEGRATOR_APP_NAME, {"opensearch-index-name": "index-name"})

    # Assert:
    status = juju.wait(
        lambda status: jubilant.all_blocked(status) and jubilant.all_agents_idle(status), delay=5
    )

    # Assert:
    assert (
        "Charm waiting to be integrated with OpenSearch"
        in status.apps[KUBEFLOW_INTEGRATOR_APP_NAME].app_status.message
    )
    assert (
        "Charm waiting to be integrated with OpenSearch"
        in status.apps[KUBEFLOW_INTEGRATOR_APP_NAME]
        .units[f"{KUBEFLOW_INTEGRATOR_APP_NAME}/0"]
        .workload_status.message
    )


def test_integrate_with_opensearch(juju: jubilant.Juju):
    """Deploy OpenSearch, integrate with kubeflow-integrator. The charm should be in an active state with configured index in the relation."""
    logger.info("Deploying OpenSearch charm")

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

    # Wait for opensearch to be active

    juju.wait(
        lambda status: jubilant.all_active(
            status, OPENSEARCH_APP_NAME, SELF_SIGNED_CERTIFICATES_APP_NAME
        )
        and jubilant.all_agents_idle(
            status, OPENSEARCH_APP_NAME, SELF_SIGNED_CERTIFICATES_APP_NAME
        ),
        delay=5,
    )

    logger.info("Integrate opensearch with kubeflow-integrator")
    juju.integrate(OPENSEARCH_APP_NAME, KUBEFLOW_INTEGRATOR_APP_NAME)

    # Wat for kubeflow-integrator to be active
    juju.wait(
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR_APP_NAME)
        and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR_APP_NAME)
    )

    logger.info("Validating relation data")

    # Check relation data and make sure index and credentials are there
    relation_data = get_application_data(
        juju, KUBEFLOW_INTEGRATOR_APP_NAME, OPENSEARCH_RELATION_NAME
    )
    # Make sure we have a relation with data
    assert len(relation_data) > 0
    opensearch_rel_data = list(relation_data.items())[0][1]
    assert "index" in opensearch_rel_data["data"]
    assert "secret-user" in opensearch_rel_data


def test_integrate_with_opensearch_without_config(juju: jubilant.Juju):
    """Test removing the relation, removing the `opensearch-index-name` config option, then integrate again with opensearch. The charm should be in a blocked status, complaining about a missing config option."""
    logger.info("Removing relation with opensearch")
    # Remove relation of opensearch and kubeflow-integrator
    juju.remove_relation(OPENSEARCH_APP_NAME, KUBEFLOW_INTEGRATOR_APP_NAME)

    logger.info("Resetting 'opensearch-index-name' config option")
    # Remove opensearch-index-name config option
    juju.config(KUBEFLOW_INTEGRATOR_APP_NAME, reset=["opensearch-index-name"])

    logger.info("Waiting for kubeflow-integrator to be active")
    # Wat for kubeflow-integrator to be active
    juju.wait(
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR_APP_NAME)
        and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR_APP_NAME)
    )

    logger.info("Integrate opensearch with kubeflow-integrator")
    # Integrate with opensearch
    juju.integrate(OPENSEARCH_APP_NAME, KUBEFLOW_INTEGRATOR_APP_NAME)

    # Wait for kubeflow-integrator to be blocked
    logger.info("Waiting for kubeflow-integrator to be blocked since a config option is missing")
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR_APP_NAME)
        and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR_APP_NAME),
        delay=5,
    )

    assert (
        "Missing config(s): 'opensearch-index-name'"
        in status.apps[KUBEFLOW_INTEGRATOR_APP_NAME].app_status.message
    )
    assert (
        "Missing config(s): 'opensearch-index-name'"
        in status.apps[KUBEFLOW_INTEGRATOR_APP_NAME]
        .units[f"{KUBEFLOW_INTEGRATOR_APP_NAME}/0"]
        .workload_status.message
    )
