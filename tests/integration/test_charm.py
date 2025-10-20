#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
KUBEFLOW_INTEGRATOR = "kubeflow-integrator"


def test_deploy_and_config(juju: jubilant.Juju, kubeflow_integrator: str):
    """Test deploying the kubeflow integrator charm.

    The charm should be in a blocked status since `profile` config option is not specified. The charm should be in an active state once the config option is specified.
    """
    logger.info("Deploying Kubeflow Integrator charm")
    # When:
    juju.deploy(kubeflow_integrator, app=KUBEFLOW_INTEGRATOR)

    status = juju.wait(
        lambda status: jubilant.any_blocked(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )
    # Assert:
    assert "Missing config(s): 'profile'" in status.apps[KUBEFLOW_INTEGRATOR].app_status.message

    logger.info("Configuring the profile config option")
    # When:
    juju.config(KUBEFLOW_INTEGRATOR, {"profile": "profile-name"})

    # Assert:
    juju.wait(
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )


def test_deploy_invalid_config(juju: jubilant.Juju, kubeflow_integrator: str):
    """Test deploying the kubeflow integrator charm using an invalid profile."""
    logger.info("Configuring the profile config option with invalid profile")
    # When:
    juju.config(KUBEFLOW_INTEGRATOR, {"profile": "-profile"})

    # Assert:
    status = juju.wait(
        lambda status: jubilant.any_blocked(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )

    assert "Invalid config(s): 'profile'" in status.apps[KUBEFLOW_INTEGRATOR].app_status.message
