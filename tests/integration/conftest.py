#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import os
from pathlib import Path

import jubilant
import pytest
import yaml
from typing_extensions import Literal

logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
MICROK8S_CLOUD = "microk8s-cloud"
CONCIERGE_MICROK8S_CONTROLLER = "concierge-microk8s"
MICROK8S_MODEL_NAME = "microk8s-model"


def pytest_addoption(parser):
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )


@pytest.fixture
def ubuntu_base() -> str | None:
    """Charm base version to use for testing."""
    return os.environ.get("CHARM_UBUNTU_BASE", None)


@pytest.fixture
def kubeflow_integrator(ubuntu_base: str | None) -> Path:
    """Path to the packed kf-integrator charm."""
    # Fetch the packed charm
    if not ubuntu_base:
        raise ValueError("`CHARM_UBUNTU_BASE` environment variable not specified.")
    packed_charm = f"{APP_NAME}_ubuntu@{ubuntu_base}-amd64.charm"
    if not (path := next(iter(Path.cwd().glob(packed_charm)), None)):
        raise FileNotFoundError("Could not find packed kubeflow-integrator charm.")

    return path


# @pytest.fixture(scope="module")
# def juju(request: pytest.FixtureRequest):
#     keep_models = bool(request.config.getoption("--keep-models"))

#     with jubilant.temp_model(keep=keep_models) as juju:
#         juju.wait_timeout = 10 * 60

#         yield juju  # run the test

#         if request.session.testsfailed:
#             log = juju.debug_log(limit=30)
#             print(log, end="")


def get_cloud_names(cloud_type: Literal["lxd", "k8s"]) -> str | None:
    """Gets controller name for specified cloud, i.e. localhost, microk8s."""
    clouds = json.loads(jubilant.Juju().cli("clouds", "--format", "json", include_model=False))

    for cloud_name in clouds:
        if clouds[cloud_name].get("type") == cloud_type:
            return cloud_name
    return None


def get_controller_name(cloud_type: Literal["lxd", "k8s"]) -> str | None:
    """Gets controller name for specified cloud, i.e. localhost, microk8s."""
    clouds = json.loads(jubilant.Juju().cli("clouds", "--format", "json", include_model=False))
    cloud_names = [name for name in clouds if clouds[name].get("type") == cloud_type]

    res = json.loads(jubilant.Juju().cli("controllers", "--format", "json", include_model=False))
    for controller in res.get("controllers", {}):
        if res["controllers"][controller].get("cloud") in cloud_names:
            return controller
    return None


@pytest.fixture(scope="module")
def vm_controller() -> str | None:
    """Returns the lxd controller name or None if not available."""
    return get_controller_name("lxd")


@pytest.fixture(scope="module")
def k8s_controller() -> str | None:
    """Returns the microk8s controller name or None if not available."""
    return get_controller_name("k8s")


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest, k8s_controller: str):
    keep_models = bool(request.config.getoption("--keep-models"))
    with jubilant.temp_model(keep=keep_models, controller=k8s_controller) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=30)
            print(log, end="")


@pytest.fixture(scope="module")
def juju_vm(request: pytest.FixtureRequest, vm_controller: str):
    keep_models = bool(request.config.getoption("--keep-models"))
    with jubilant.temp_model(keep=keep_models, controller=vm_controller) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=30)
            print(log, end="")
