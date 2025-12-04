#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import base64
import json
import logging
import subprocess
from pathlib import Path
from typing import Type

import jubilant
import lightkube
import pytest
import yaml
from helpers import get_application_data
from lightkube import ALL_NS
from lightkube.generic_resource import create_namespaced_resource
from lightkube.models.core_v1 import Container, PodSpec
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.resources.core_v1 import Namespace, Pod, Secret, ServiceAccount
from lightkube.resources.rbac_authorization_v1 import Role, RoleBinding
from tenacity import Retrying, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

KUBEFLOW_INTEGRATOR = "kubeflow-integrator"
PROFILE_CONFIG_VALUE = "testing"
SPARK_SERVICE_ACCOUNT_CONFIG_VALUE = "spark"
KUBEFLOW_SPARK_RELATION_NAME = "spark"

METACONTROLLER_CHARM = "metacontroller-operator"
METACONTROLLER_CHARM_CHANNEL = "latest/edge"
RESOURCE_DISPATCHER = "resource-dispatcher"
RESOURCE_DISPATCHER_CHANNEL = "latest/edge"
ADMISSION_WEBHOOK = "admission-webhook"
ADMISSION_WEBHOOK_CHANNEL = "latest/edge"
SPARK_INTEGRATION_HUB = "spark-integration-hub-k8s"
SPARK_INTEGRATION_HUB_CHANNEL = "3/edge"

SECRETS_RELATION_NAME = "secrets"
SERVICE_ACCOUNTS_RELATION_NAME = "service-accounts"
ROLES_RELATION_NAME = "roles"
ROLE_BINDINGS_RELATION_NAME = "role-bindings"
POD_DEFAULTS_RELATION_NAME = "pod-defaults"

EXPECTED_SERVICE_ACCOUNT_NAME = SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
EXPECTED_SECRET_NAME = f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"
EXPECTED_ROLE_NAME = f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role"
EXPECTED_ROLEBINDING_NAME = f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role-binding"
EXPECTED_NOTEBOOK_PODDEFAULT_NAME = "pyspark-notebook"
EXPECTED_PIPELINE_PODDEFAULT_NAME = "pyspark-pipeline"

SPARK_IMAGE = "ghcr.io/canonical/charmed-spark:3.5.5-22.04_edge"
SPARK_EXAMPLES_JAR = "spark-examples_2.12-3.5.5.jar"

SPARK_DRIVER_PORT = 37371
SPARK_BLOCK_MANAGER_PORT = 6060
SPARK_PIPELINE_SELECTOR_LABEL = "access-spark-pipeline"
SPARK_NOTEBOOK_SELECTOR_LABEL = "access-spark-notebook"

POD_DEFAULT = create_namespaced_resource(
    group="kubeflow.org", version="v1alpha1", kind="PodDefault", plural="poddefaults"
)


@pytest.fixture(scope="session")
def lightkube_client() -> lightkube.Client:
    client = lightkube.Client(field_manager=RESOURCE_DISPATCHER)
    return client


@pytest.fixture(scope="session")
def kubeflow_enabled_namespace(lightkube_client: lightkube.Client):
    """Return a new namespace with the label user.kubeflow.org/enabled=true."""
    namespace_name = "kubeflow-enabled-namespace"
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
    """Deploy the kubeflow integrator charm and configure it for Spark integration."""
    logger.info("Deploying Kubeflow Integrator charm...")
    juju.deploy(
        kubeflow_integrator,
        app=KUBEFLOW_INTEGRATOR,
        config={
            "profile": PROFILE_CONFIG_VALUE,
            "spark-service-account": SPARK_SERVICE_ACCOUNT_CONFIG_VALUE,
        },
    )
    status = juju.wait(
        lambda status: jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_agents_idle(status),
        delay=5,
    )
    assert "Missing relation with: Spark" in status.apps[KUBEFLOW_INTEGRATOR].app_status.message


def test_integrate_with_spark_integration_hub(juju: jubilant.Juju):
    """Integrate the kubeflow integrator charm with spark integration hub charm."""
    logger.info("Deploying spark-integration-hub-k8s charm...")
    juju.deploy(
        SPARK_INTEGRATION_HUB,
        app=SPARK_INTEGRATION_HUB,
        channel=SPARK_INTEGRATION_HUB_CHANNEL,
        trust=True,
    )
    juju.wait(
        lambda status: jubilant.all_blocked(status, KUBEFLOW_INTEGRATOR)
        and jubilant.all_active(status, SPARK_INTEGRATION_HUB)
        and jubilant.all_agents_idle(status),
        delay=5,
    )

    logger.info("Integrating kubeflow integrator with spark-integration-hub...")
    juju.integrate(KUBEFLOW_INTEGRATOR, SPARK_INTEGRATION_HUB)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_resource_manifest_in_integration_hub_relation(juju: jubilant.Juju):
    """Check that the integration-hub is indeed sharing resource manifests to the kubeflow-integrator."""
    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=KUBEFLOW_INTEGRATOR, relation_name=KUBEFLOW_SPARK_RELATION_NAME
            ).values()
        )
    )
    secret_extra = relation_data["secret-extra"]
    secret_content = juju.show_secret(identifier=secret_extra, reveal=True).content
    resource_manifest = secret_content["resource-manifest"]
    resources = list(yaml.safe_load_all(resource_manifest))

    assert any(
        res
        for res in resources
        if res["kind"] == "Secret"
        and res["metadata"]["name"] == f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"
    )
    assert any(
        res
        for res in resources
        if res["kind"] == "ServiceAccount"
        and res["metadata"]["name"] == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
    )
    assert any(
        res
        for res in resources
        if res["kind"] == "Role" and res["metadata"]["name"] == EXPECTED_ROLE_NAME
    )
    assert any(
        res
        for res in resources
        if res["kind"] == "RoleBinding"
        and res["metadata"]["name"] == f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role-binding"
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


@pytest.mark.parametrize(
    "relation_name,resource_type,resource_class,resource_names,count",
    [
        ("secrets", "Secret", Secret, [EXPECTED_SECRET_NAME], 1),
        ("service-accounts", "ServiceAccount", ServiceAccount, [EXPECTED_SERVICE_ACCOUNT_NAME], 1),
        ("roles", "Role", Role, [EXPECTED_ROLE_NAME], 1),
        ("role-bindings", "RoleBinding", RoleBinding, [EXPECTED_ROLEBINDING_NAME], 1),
        (
            "pod-defaults",
            "PodDefault",
            POD_DEFAULT,
            [EXPECTED_NOTEBOOK_PODDEFAULT_NAME, EXPECTED_PIPELINE_PODDEFAULT_NAME],
            2,
        ),
    ],
)
def test_resource_dispatcher_relations(
    juju: jubilant.Juju,
    lightkube_client: lightkube.Client,
    kubeflow_enabled_namespace: str,
    relation_name: str,
    resource_type: str,
    resource_class: Type,
    resource_names: str,
    count: int,
):
    """Integrate the kubeflow integrator with resource dispatcher over the various relations and check that the resources are created."""
    res_before_relation: list = list(lightkube_client.list(resource_class, namespace=ALL_NS))
    assert not any(
        res.metadata
        and res.metadata.name in resource_names
        and res.metadata.namespace == kubeflow_enabled_namespace
        for res in res_before_relation
    )

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

    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=relation_name
            ).values()
        )
    )
    res_in_relation_data = json.loads(relation_data["kubernetes_manifests"])
    assert (
        len(
            [
                res
                for res in res_in_relation_data
                if res["kind"] == resource_type and res["metadata"]["name"] in resource_names
            ]
        )
        == count
    )

    # Try checking for the resources with retries since resource-dispatcher may take some time to create them
    for attempt in Retrying(stop=stop_after_attempt(30), wait=wait_fixed(10)):
        with attempt:
            res_after_relation = list(lightkube_client.list(resource_class, namespace=ALL_NS))
            matching_res = [
                res
                for res in res_after_relation
                if res.metadata
                and res.metadata.name in resource_names
                and res.metadata.namespace == kubeflow_enabled_namespace
            ]
            assert (
                len(matching_res) == count
            ), f"Expected exactly {count} matching {resource_type} after relation is created."


def test_rolebinding_subject_properly_applied(
    kubeflow_enabled_namespace: str, lightkube_client: lightkube.Client
):
    """Test that the RoleBinding created has the proper subject pointing to the correct ServiceAccount in correct namespace."""
    spark_role_binding = lightkube_client.get(
        RoleBinding,
        name=EXPECTED_ROLEBINDING_NAME,
        namespace=kubeflow_enabled_namespace,
    )
    assert spark_role_binding.subjects is not None
    assert len(spark_role_binding.subjects) == 1
    subject = spark_role_binding.subjects[0]
    assert subject.kind == "ServiceAccount"
    assert subject.name == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
    assert subject.namespace == kubeflow_enabled_namespace


@pytest.mark.parametrize(
    "pod_name,selector_label",
    [
        ("notebook", SPARK_NOTEBOOK_SELECTOR_LABEL),
        ("pipeline", SPARK_PIPELINE_SELECTOR_LABEL),
    ],
)
def test_pod_default_gets_applied(
    kubeflow_enabled_namespace: str,
    lightkube_client: lightkube.Client,
    pod_name: str,
    selector_label: str,
):
    """Test that the PodDefault created by the kubeflow integrator is properly applied to a pod with the matching selector label."""
    lightkube_client.create(
        Pod(
            metadata=ObjectMeta(
                name=pod_name,
                namespace=kubeflow_enabled_namespace,
                labels={selector_label: "true"},
            ),
            spec=PodSpec(
                containers=[
                    Container(
                        name="nginx",
                        image="nginx:latest",
                    )
                ]
            ),
        )
    )
    for attempt in Retrying(
        stop=stop_after_attempt(30),
        wait=wait_fixed(10),
    ):
        with attempt:
            notebook_pod = lightkube_client.get(
                Pod,
                name=pod_name,
                namespace=kubeflow_enabled_namespace,
            )
            assert notebook_pod.spec is not None
            container = notebook_pod.spec.containers[0]
            env_vars = {
                env.name: {"value": env.value, "valueFrom": env.valueFrom} for env in container.env
            }
            assert env_vars["SPARK_SERVICE_ACCOUNT"]["value"] == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
            assert (
                env_vars["SPARK_NAMESPACE"]["valueFrom"].fieldRef.fieldPath == "metadata.namespace"
            )

            if pod_name == "notebook":
                assert container.args == [
                    "--namespace",
                    "$SPARK_NAMESPACE",
                    "--username",
                    "$SPARK_SERVICE_ACCOUNT",
                    "--conf",
                    f"spark.driver.port={SPARK_DRIVER_PORT}",
                    "--conf",
                    f"spark.blockManager.port={SPARK_BLOCK_MANAGER_PORT}",
                ]
                annotations = notebook_pod.metadata.annotations
                assert (
                    annotations["traffic.sidecar.istio.io/excludeInboundPorts"]
                    == f"{SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}"
                )
                assert (
                    annotations["traffic.sidecar.istio.io/excludeOutboundPorts"]
                    == f"{SPARK_DRIVER_PORT},{SPARK_BLOCK_MANAGER_PORT}"
                )


def test_change_in_spark_properties_reflected(
    juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str
):
    """Test that a change in Spark properties by changing an config option integration hub is reflected all the way to the K8s resources."""
    secret_before_changes = lightkube_client.get(
        Secret, name=EXPECTED_SECRET_NAME, namespace=kubeflow_enabled_namespace
    )
    spark_properties_before_changes = secret_before_changes.data
    assert spark_properties_before_changes is None

    logger.info("Enabling dynamic pod allocation in Spark Integration Hub...")
    juju.config(SPARK_INTEGRATION_HUB, {"enable-dynamic-allocation": "true"})
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    secret_after_changes = lightkube_client.get(
        Secret, name=EXPECTED_SECRET_NAME, namespace=kubeflow_enabled_namespace
    )
    spark_properties_after_changes = secret_after_changes.data
    assert spark_properties_after_changes is not None
    decoded_spark_properties = {
        key: base64.b64decode(value).decode("utf-8")
        for key, value in spark_properties_after_changes.items()
    }
    assert decoded_spark_properties["spark.dynamicAllocation.enabled"] == "true"
    assert decoded_spark_properties["spark.dynamicAllocation.minExecutors"] == "1"
    assert decoded_spark_properties["spark.dynamicAllocation.shuffleTracking.enabled"] == "true"


def test_read_spark_config_using_spark_client(kubeflow_enabled_namespace: str):
    """Test that we can read the spark config using a spark-client snap."""
    logger.info("Reading Spark properties using spark-client snap...")
    process = subprocess.run(
        [
            "spark-client.service-account-registry",
            "get-config",
            "--namespace",
            kubeflow_enabled_namespace,
            "--username",
            SPARK_SERVICE_ACCOUNT_CONFIG_VALUE,
        ],
        capture_output=True,
        text=True,
    )
    assert (
        process.returncode == 0
    ), f"spark-client.service-account-registry get-config command failed with error: {process.stderr}"
    output = process.stdout.strip().splitlines()
    assert "spark.dynamicAllocation.enabled=true" in output
    assert "spark.dynamicAllocation.minExecutors=1" in output
    assert "spark.dynamicAllocation.shuffleTracking.enabled=true" in output


def test_run_spark_job_using_spark_client(
    lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str
):
    """Test that we can run a spark job using the spark-client snap and the created service account."""
    pods_before_spark_job = lightkube_client.list(Pod, namespace=kubeflow_enabled_namespace)
    driver_pods_before_spark_job = [
        pod.metadata.name
        for pod in pods_before_spark_job
        if pod.metadata
        and pod.metadata.name
        and pod.metadata.name.startswith("org-apache-spark-examples-sparkpi-")
        and pod.metadata.name.endswith("-driver")
    ]

    logger.info("Running Spark job using spark-client snap...")
    process = subprocess.run(
        [
            "spark-client.spark-submit",
            "--namespace",
            kubeflow_enabled_namespace,
            "--username",
            SPARK_SERVICE_ACCOUNT_CONFIG_VALUE,
            "--class",
            "org.apache.spark.examples.SparkPi",
            "--deploy-mode",
            "cluster",
            "--conf",
            f"spark.kubernetes.container.image={SPARK_IMAGE}",
            f"local:///opt/spark/examples/jars/{SPARK_EXAMPLES_JAR}",
            "1000",
        ],
        capture_output=True,
        text=True,
    )
    assert (
        process.returncode == 0
    ), f"spark-client.spark-submit command failed with error: {process.stderr}"

    pods_after_spark_job = lightkube_client.list(Pod, namespace=kubeflow_enabled_namespace)
    driver_pods_after_spark_job = [
        pod.metadata.name
        for pod in pods_after_spark_job
        if pod.metadata
        and pod.metadata.name
        and pod.metadata.name.startswith("org-apache-spark-examples-sparkpi-")
        and pod.metadata.name.endswith("-driver")
    ]
    new_driver_pods = set(driver_pods_after_spark_job) - set(driver_pods_before_spark_job)
    assert (
        len(new_driver_pods) == 1
    ), "Expected exactly one new Spark driver pod after submitting the Spark job."
    spark_driver_pod_name = new_driver_pods.pop()

    driver_pod_logs = list(
        lightkube_client.log(spark_driver_pod_name, namespace=kubeflow_enabled_namespace)
    )
    assert any(
        "Pi is roughly 3.14" in line for line in driver_pod_logs
    ), "Expected to find 'Pi is roughly 3.14' in Spark driver pod logs."
