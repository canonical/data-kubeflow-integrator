#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
from pathlib import Path

import os
from typing import Any, Generator
import yaml
import jubilant
import pytest
import time
import json
import subprocess
import pathlib
from tenacity import Retrying, wait_fixed, stop_after_delay

logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
MICROK8S_CLOUD = "microk8s-cloud"
MICROK8S_CONTROLLER = "k8s-controller"
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
    """charm base version to use for testing."""
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


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    keep_models = bool(request.config.getoption("--keep-models"))

    with jubilant.temp_model(keep=keep_models) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=30)
            print(log, end="")


@pytest.fixture(scope="module")
def microk8s_cloud(
    juju: jubilant.Juju, request: pytest.FixtureRequest
) -> Generator[None, Any, None]:
    """Install and configure MicroK8s as second cloud on the same juju controller.

    Skips if it configured already. Automatically removes connection to the created
    cloud and removes MicroK8s from system unless keep models parameter is used.
    """
    controller_name = next(iter(yaml.safe_load(juju.cli("show-controller", include_model=False))))

    clouds = json.loads(juju.cli("clouds", "--format=json", include_model=False))
    if f"cloud-{MICROK8S_CLOUD}" in clouds:
        yield None
        return

    try:
        subprocess.run(["sudo", "snap", "install", "--classic", "microk8s"], check=True)
        subprocess.run(["sudo", "snap", "install", "--classic", "kubectl"], check=True)
        subprocess.run(["sudo", "microk8s", "enable", "dns"], check=True)
        subprocess.run(["sudo", "microk8s", "enable", "hostpath-storage"], check=True)
        subprocess.run(
            ["sudo", "microk8s", "enable", "metallb:10.64.140.43-10.64.140.49"],
            check=True,
        )

        # Configure kubectl now
        subprocess.run(["mkdir", "-p", str(pathlib.Path.home() / ".kube")], check=True)
        kubeconfig = subprocess.check_output(["sudo", "microk8s", "config"])
        with open(str(pathlib.Path.home() / ".kube" / "config"), "w") as f:
            f.write(kubeconfig.decode())
        for attempt in Retrying(stop=stop_after_delay(150), wait=wait_fixed(15)):
            with attempt:
                if (
                    len(
                        subprocess.check_output(
                            "kubectl get po -A  --field-selector=status.phase!=Running",
                            shell=True,
                            stderr=subprocess.DEVNULL,
                        ).decode()
                    )
                    != 0
                ):  # We got sth different from "No resources found." in stderr
                    raise Exception()

        # Add microk8s to the kubeconfig
        juju.cli(
            "add-k8s",
            MICROK8S_CLOUD,
            "--client",
            "--controller",
            controller_name,
            include_model=False,
        )
    except subprocess.CalledProcessError as e:
        pytest.exit(str(e))

    yield None
    keep_models = bool(request.config.getoption("--keep-models"))

    if not keep_models:
        juju.cli(
            "remove-cloud",
            "--client",
            "--controller",
            controller_name,
            MICROK8S_CLOUD,
            include_model=False,
        )
        subprocess.run(["sudo", "snap", "remove", "--purge", "microk8s"], check=True)
        subprocess.run(["sudo", "snap", "remove", "--purge", "kubectl"], check=True)


@pytest.fixture(scope="module")
def microk8s_model(
    juju: jubilant.Juju, microk8s_cloud: None, request: pytest.FixtureRequest
) -> Generator[str, Any, None]:
    """Create new Juju model on the connected MicroK8s cloud.

    Automatically destroys that model unless keep models parameter is used.

    Returns:
        Connected Juju model.
    """

    model_name = MICROK8S_MODEL_NAME
    controller_models = juju.cli("list-models", include_model=False)
    temp_model = juju.model
    if model_name not in controller_models:
        juju.add_model(model_name, cloud=MICROK8S_CLOUD)
    if temp_model:
        juju.model = temp_model

    yield model_name

    keep_models = bool(request.config.getoption("--keep-models"))
    if not keep_models:
        juju.destroy_model(model=model_name, destroy_storage=True, force=True)
        while model_name in juju.cli("list-models", include_model=False):
            time.sleep(5)
