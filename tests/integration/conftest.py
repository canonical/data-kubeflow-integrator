#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
from pathlib import Path

import jubilant
import lightkube
import pytest
import yaml
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Namespace
from typing_extensions import Literal

logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


KUBEFLOW_USER_PROFILE_A = "kubeflow-profile-a"
KUBEFLOW_USER_PROFILE_B = "kubeflow-profile-b"
RESOURCE_DISPATCHER = "resource-dispatcher"

WAIT_TIMEOUT = 20 * 60


def pytest_addoption(parser):
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )
    parser.addoption(
        "--model",
        action="store",
        default=None,
        help="Juju model to use; if not provided, a new model "
        "will be created for each test which requires one",
    )
    parser.addoption(
        "--charm-path",
        action="store",
        default=None,
        help="Path to charm file for performing tests on.",
    )


@pytest.fixture
def kubeflow_integrator(request: pytest.FixtureRequest) -> Path:
    """Path to the packed kf-integrator charm."""
    if charm_path := request.config.getoption("--charm-path"):
        # Resolve to an absolute path so `juju deploy` treats it unambiguously as a local
        # charm
        path = Path(charm_path).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Charm file not found at --charm-path: {path}")
        return path

    if not (path := next(iter(Path.cwd().glob(f"{APP_NAME}*.charm")), None)):
        raise FileNotFoundError("Could not find packed kubeflow-integrator charm.")

    return path


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
    model_name = request.config.getoption("--model")

    def print_debug_log(juju_instance: jubilant.Juju):
        if request.session.testsfailed:
            print(f"[DEBUG] Fetching debug log for model: {juju_instance.model}")
            log = juju_instance.debug_log(limit=1000)
            print(log, end="")

    if model_name:
        juju_instance = jubilant.Juju(model=f"{k8s_controller}:{model_name}")
        juju_instance.wait_timeout = WAIT_TIMEOUT
        try:
            yield juju_instance
        finally:
            print_debug_log(juju_instance)
    else:
        with jubilant.temp_model(keep=keep_models, controller=k8s_controller) as juju_instance:
            juju_instance.wait_timeout = WAIT_TIMEOUT
            try:
                yield juju_instance
            finally:
                print_debug_log(juju_instance)


@pytest.fixture(scope="module")
def juju_vm(request: pytest.FixtureRequest, vm_controller: str):
    keep_models = bool(request.config.getoption("--keep-models"))
    model_name = request.config.getoption("--model")

    def print_debug_log(juju_instance: jubilant.Juju):
        if request.session.testsfailed:
            print(f"[DEBUG] Fetching debug log for model: {juju_instance.model}")
            log = juju_instance.debug_log(limit=1000)
            print(log, end="")

    if model_name:
        juju_instance = jubilant.Juju(model=model_name)
        juju_instance.wait_timeout = WAIT_TIMEOUT
        try:
            yield juju_instance
        finally:
            print_debug_log(juju_instance)
    else:
        with jubilant.temp_model(keep=keep_models, controller=vm_controller) as juju_instance:
            juju_instance.wait_timeout = WAIT_TIMEOUT
            try:
                yield juju_instance
            finally:
                print_debug_log(juju_instance)


@pytest.fixture(scope="session")
def lightkube_client() -> lightkube.Client:
    client = lightkube.Client(field_manager=RESOURCE_DISPATCHER)
    return client


@pytest.fixture(scope="module")
def kubeflow_user_profile_a(lightkube_client: lightkube.Client):
    """Return a new namespace with the label user.kubeflow.org/enabled=true."""
    namespace_name = KUBEFLOW_USER_PROFILE_A
    namespace = Namespace(
        metadata=ObjectMeta(
            name=namespace_name,
            labels={
                "user.kubeflow.org/enabled": "true",
                "app.kubernetes.io/part-of": "kubeflow-profile",
            },
        )
    )
    logger.info(
        f"Creating namespace {namespace_name} with label user.kubeflow.org/enabled=true ..."
    )
    lightkube_client.create(namespace)
    assert namespace.metadata
    yield namespace.metadata.name
    lightkube_client.delete(Namespace, name=namespace_name)


@pytest.fixture(scope="module")
def kubeflow_user_profile_b(lightkube_client: lightkube.Client):
    """Return a new namespace with the label user.kubeflow.org/enabled=true."""
    namespace_name = KUBEFLOW_USER_PROFILE_B
    namespace = Namespace(
        metadata=ObjectMeta(
            name=namespace_name,
            labels={
                "user.kubeflow.org/enabled": "true",
                "app.kubernetes.io/part-of": "kubeflow-profile",
            },
        )
    )
    logger.info(
        f"Creating namespace {namespace_name} with label user.kubeflow.org/enabled=true ..."
    )
    lightkube_client.create(namespace)
    assert namespace.metadata
    yield namespace.metadata.name
    lightkube_client.delete(Namespace, name=namespace_name)
