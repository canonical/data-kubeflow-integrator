#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from typing import Type

import jubilant
import lightkube
import pytest
import yaml
from lightkube import ALL_NS
from lightkube.generic_resource import create_namespaced_resource
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Namespace, Secret, ServiceAccount
from lightkube.resources.rbac_authorization_v1 import Role, RoleBinding
from tenacity import Retrying, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

KUBEFLOW_INTEGRATOR = "kubeflow-integrator"
SPARK_SERVICE_ACCOUNT_CONFIG_VALUE = "spark"

KUBEFLOW_USER_PROFILE_A = "kubeflow-profile-a"
KUBEFLOW_USER_PROFILE_B = "kubeflow-profile-b"

METACONTROLLER_CHARM = "metacontroller-operator"
METACONTROLLER_CHARM_CHANNEL = "latest/edge"
RESOURCE_DISPATCHER = "resource-dispatcher"
RESOURCE_DISPATCHER_CHANNEL = "latest/edge"
ADMISSION_WEBHOOK = "admission-webhook"
ADMISSION_WEBHOOK_CHANNEL = "latest/edge"
SPARK_INTEGRATION_HUB = "spark-integration-hub-k8s"
SPARK_INTEGRATION_HUB_CHANNEL = "3/edge"

EXPECTED_SERVICE_ACCOUNT_NAME = SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
EXPECTED_SECRET_NAME = f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"
EXPECTED_ROLE_NAME = f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role"
EXPECTED_ROLEBINDING_NAME = f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role-binding"
EXPECTED_NOTEBOOK_PODDEFAULT_NAME = "pyspark-notebook"
EXPECTED_PIPELINE_PODDEFAULT_NAME = "pyspark-pipeline"

POD_DEFAULT = create_namespaced_resource(
    group="kubeflow.org", version="v1alpha1", kind="PodDefault", plural="poddefaults"
)


@pytest.fixture(scope="session")
def lightkube_client() -> lightkube.Client:
    client = lightkube.Client(field_manager=RESOURCE_DISPATCHER)
    return client


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
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


def test_deploy_and_configure_kf_integrator(juju: jubilant.Juju, kubeflow_integrator: str):
    """Deploy the kubeflow integrator charm and configure it for profile testing."""
    logger.info("Deploying Kubeflow Integrator charm...")
    juju.deploy(
        kubeflow_integrator,
        app=KUBEFLOW_INTEGRATOR,
        config={
            "profile": KUBEFLOW_USER_PROFILE_A,
            "spark-service-account": SPARK_SERVICE_ACCOUNT_CONFIG_VALUE,
        },
    )
    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR) and jubilant.all_agents_idle(status)
        ),
        delay=5,
    )


def test_integrate_with_spark_integration_hub(juju: jubilant.Juju):
    """Deploy and integrate the spark integration hub charm."""
    logger.info("Deploying spark-integration-hub-k8s charm...")
    juju.deploy(
        SPARK_INTEGRATION_HUB,
        app=SPARK_INTEGRATION_HUB,
        channel=SPARK_INTEGRATION_HUB_CHANNEL,
        trust=True,
    )
    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
            and jubilant.all_active(status, SPARK_INTEGRATION_HUB)
            and jubilant.all_agents_idle(status)
        ),
        delay=5,
    )

    logger.info("Integrating kubeflow integrator with spark-integration-hub...")
    juju.integrate(KUBEFLOW_INTEGRATOR, SPARK_INTEGRATION_HUB)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_deploy_resource_dispatcher_setup(juju: jubilant.Juju):
    """Deploy the necessary setup for the resource dispatcher."""
    logger.info("Deploying metacontroller-operator charm...")
    juju.deploy(METACONTROLLER_CHARM, channel=METACONTROLLER_CHARM_CHANNEL, trust=True)
    juju.deploy(ADMISSION_WEBHOOK, channel=ADMISSION_WEBHOOK_CHANNEL, trust=True)
    juju.deploy(
        RESOURCE_DISPATCHER,
        channel=RESOURCE_DISPATCHER_CHANNEL,
        trust=True,
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_integrate_resource_dispatcher(
    juju: jubilant.Juju,
    lightkube_client: lightkube.Client,
    kubeflow_user_profile_a: str,
):
    """Integrate the kubeflow integrator with resource dispatcher over all relations."""
    for relation_name in [
        "secrets",
        "service-accounts",
        "roles",
        "role-bindings",
        "pod-defaults",
    ]:
        logger.info(
            f"Integrating kubeflow-integrator <> resource-dispatcher over `{relation_name}` endpoint..."
        )
        juju.integrate(
            f"{KUBEFLOW_INTEGRATOR}:{relation_name}",
            f"{RESOURCE_DISPATCHER}:{relation_name}",
        )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # Wait for resources to appear in profile A
    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        with attempt:
            resources = list(lightkube_client.list(Secret, namespace=ALL_NS))
            matching = [
                res
                for res in resources
                if res.metadata
                and res.metadata.name == EXPECTED_SECRET_NAME
                and res.metadata.namespace == kubeflow_user_profile_a
            ]
            assert len(matching) == 1, "Expected Secret to be created in profile A."


def test_change_active_profile_from_a_to_b(
    juju: jubilant.Juju,
    kubeflow_user_profile_b: str,
):
    """Change the active profile by changing the profile config option and see that the resources are deleted from old profile and created in new profile."""
    logger.info("Changing active profile from profile A to profile B...")

    # To change the profile, the integration hub relation needs to be torn down first
    juju.remove_relation(KUBEFLOW_INTEGRATOR, SPARK_INTEGRATION_HUB)
    juju.wait(
        lambda status: (
            jubilant.all_active(
                status,
                SPARK_INTEGRATION_HUB,
                RESOURCE_DISPATCHER,
                ADMISSION_WEBHOOK,
                METACONTROLLER_CHARM,
            )
            and jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
            and jubilant.all_agents_idle(status)
        ),
        delay=5,
    )

    # Now change the active profile to profile B
    juju.config(KUBEFLOW_INTEGRATOR, {"profile": kubeflow_user_profile_b})

    # And integrate the integration hub relation again to trigger the creation of resources in the new profile
    juju.integrate(KUBEFLOW_INTEGRATOR, SPARK_INTEGRATION_HUB)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


@pytest.mark.parametrize(
    "resource_type,resource_class,resource_names,count",
    [
        ("Secret", Secret, [EXPECTED_SECRET_NAME], 1),
        ("ServiceAccount", ServiceAccount, [EXPECTED_SERVICE_ACCOUNT_NAME], 1),
        ("Role", Role, [EXPECTED_ROLE_NAME], 1),
        ("RoleBinding", RoleBinding, [EXPECTED_ROLEBINDING_NAME], 1),
        (
            "PodDefault",
            POD_DEFAULT,
            [EXPECTED_NOTEBOOK_PODDEFAULT_NAME, EXPECTED_PIPELINE_PODDEFAULT_NAME],
            2,
        ),
    ],
)
def test_resources_deleted_from_old_profile_and_created_in_new_profile(
    lightkube_client: lightkube.Client,
    kubeflow_user_profile_a: str,
    kubeflow_user_profile_b: str,
    resource_type: str,
    resource_class: Type,
    resource_names: str,
    count: int,
):
    # Try checking for the resources with retries since resource-dispatcher may take some time to create/delete them
    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        with attempt:
            resources = list(lightkube_client.list(resource_class, namespace=ALL_NS))
            matching_res_a = [
                res
                for res in resources
                if res.metadata
                and res.metadata.name in resource_names
                and res.metadata.namespace == kubeflow_user_profile_a
            ]
            assert len(matching_res_a) == 0, (
                f"Expected exactly 0 matching {resource_type} in profile A after relation is created."
            )

            matching_res_b = [
                res
                for res in resources
                if res.metadata
                and res.metadata.name in resource_names
                and res.metadata.namespace == kubeflow_user_profile_b
            ]
            assert len(matching_res_b) == count, (
                f"Expected exactly {count} matching {resource_type} in profile B after relation is created."
            )


def test_enable_wildcard_profile(
    juju: jubilant.Juju,
):
    """Change the active profile to wildcard (*)."""
    logger.info("Changing active profile from profile B to wildcard...")

    # To change the profile, the integration hub relation needs to be torn down first
    juju.remove_relation(KUBEFLOW_INTEGRATOR, SPARK_INTEGRATION_HUB)
    juju.wait(
        lambda status: (
            jubilant.all_active(
                status,
                SPARK_INTEGRATION_HUB,
                RESOURCE_DISPATCHER,
                ADMISSION_WEBHOOK,
                METACONTROLLER_CHARM,
            )
            and jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
            and jubilant.all_agents_idle(status)
        ),
        delay=5,
    )

    # Now change the active profile to wildcard
    juju.config(KUBEFLOW_INTEGRATOR, {"profile": "*"})

    # And integrate the integration hub relation again to trigger the creation of resources in the new profile
    juju.integrate(KUBEFLOW_INTEGRATOR, SPARK_INTEGRATION_HUB)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


@pytest.mark.parametrize(
    "resource_type,resource_class,resource_names,count",
    [
        ("Secret", Secret, [EXPECTED_SECRET_NAME], 1),
        ("ServiceAccount", ServiceAccount, [EXPECTED_SERVICE_ACCOUNT_NAME], 1),
        ("Role", Role, [EXPECTED_ROLE_NAME], 1),
        ("RoleBinding", RoleBinding, [EXPECTED_ROLEBINDING_NAME], 1),
        (
            "PodDefault",
            POD_DEFAULT,
            [EXPECTED_NOTEBOOK_PODDEFAULT_NAME, EXPECTED_PIPELINE_PODDEFAULT_NAME],
            2,
        ),
    ],
)
def test_resources_create_across_all_profiles(
    lightkube_client: lightkube.Client,
    kubeflow_user_profile_a: str,
    kubeflow_user_profile_b: str,
    resource_type: str,
    resource_class: Type,
    resource_names: str,
    count: int,
):
    """Test that the resources are created in both profile A and profile B when the active profile is wildcard."""
    # Try checking for the resources with retries since resource-dispatcher may take some time to create/delete them
    for attempt in Retrying(stop=stop_after_attempt(20), wait=wait_fixed(10)):
        with attempt:
            resources = list(lightkube_client.list(resource_class, namespace=ALL_NS))
            matching_res_a = [
                res
                for res in resources
                if res.metadata
                and res.metadata.name in resource_names
                and res.metadata.namespace == kubeflow_user_profile_a
            ]
            assert len(matching_res_a) == count, (
                f"Expected exactly {count} matching {resource_type} in profile A after relation is created."
            )

            matching_res_b = [
                res
                for res in resources
                if res.metadata
                and res.metadata.name in resource_names
                and res.metadata.namespace == kubeflow_user_profile_b
            ]
            assert len(matching_res_b) == count, (
                f"Expected exactly {count} matching {resource_type} in profile B after relation is created."
            )
