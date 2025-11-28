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


def test_deploy(juju: jubilant.Juju, kubeflow_integrator: str):
    """Test deploying the kubeflow integrator charm.

    The charm should be in a blocked status since `profile` config option is not specified.
    """
    logger.info("Deploying Kubeflow Integrator charm")
    juju.deploy(kubeflow_integrator, app=KUBEFLOW_INTEGRATOR)
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )
    assert "Missing config(s): 'profile'" in status.apps[KUBEFLOW_INTEGRATOR].app_status.message


def test_configure_profile(juju: jubilant.Juju):
    """Set the `profile` config option in the charm. The charm should then transition to active state."""
    logger.info("Configuring the profile config option")
    juju.config(KUBEFLOW_INTEGRATOR, {"profile": "profile-name"})
    status = juju.wait(
        lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )
    assert status.apps[KUBEFLOW_INTEGRATOR].app_status.current == "active"


def test_invalid_profile_config(juju: jubilant.Juju):
    """Set the `profile` config option to an invalid value. The charm should transition to blocked state again."""
    logger.info("Setting the profile config option to invalid value...")
    juju.config(KUBEFLOW_INTEGRATOR, {"profile": "-profile"})

    status = juju.wait(
        lambda status: jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )

    assert "Invalid config(s): 'profile'" in status.apps[KUBEFLOW_INTEGRATOR].app_status.message
