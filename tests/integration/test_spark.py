#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

import base64
import json
import logging
from pathlib import Path
import subprocess

import jubilant
import lightkube
from lightkube.resources.core_v1 import Namespace, Secret, ServiceAccount, Pod
from lightkube.generic_resource import create_namespaced_resource
from lightkube.resources.rbac_authorization_v1 import Role, RoleBinding
from lightkube.models.meta_v1 import ObjectMeta
from lightkube import ALL_NS
import pytest
import yaml
from tenacity import Retrying, stop_after_attempt, wait_fixed

from helpers import get_application_data, validate_k8s_poddefault, validate_k8s_secret

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

KUBEFLOW_INTEGRATOR = "kubeflow-integrator"
PROFILE_CONFIG_VALUE = "testing"
SPARK_SERVICE_ACCOUNT_CONFIG_VALUE = "spark"
KUBEFLOW_SPARK_RELATION_NAME = "spark"

SPARK_INTEGRATION_HUB = "spark-integration-hub-k8s"

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

POD_DEFAULT = create_namespaced_resource(
    group="kubeflow.org",
    version="v1alpha1",
    kind="PodDefault",
    plural="poddefaults"
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
            labels={"user.kubeflow.org/enabled": "true"},
        )
    )
    logger.info(f"Creating namespace {namespace_name} with label user.kubeflow.org/enabled=true ...")
    lightkube_client.create(namespace)
    assert namespace.metadata
    yield namespace.metadata.name
    lightkube_client.delete(Namespace, name=namespace_name)












OPENSEARCH_RELATION_NAME = "opensearch"

OPENSEARCH_CHARM = "opensearch"
OPENSEARCH_CHANNEL = "2/edge"
OPENSEARCH_PROFILE_CONFIG = "testing"
OPENSEARCH_APP_NAME = "opensearch"

SELF_SIGNED_CERTIFICATES_CHARM = "self-signed-certificates"
SELF_SIGNED_CERTIFICATES_CHANNEL = "latest/stable"
SELF_SIGNED_CERTIFICATES_APP_NAME = "self-signed-certificates"


METACONTROLLER_CHARM = "metacontroller-operator"
METACONTROLLER_CHARM_CHANNEL = "latest/edge"
RESOURCE_DISPATCHER = "resource-dispatcher"
RESOURCE_DISPATCHER_CHANNEL = "latest/edge"

ADMISSION_WEBHOOK_CHARM = "admission-webhook"
RESOURCE_DISPATCHER_APP_NAME = "resource-dispatcher"


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
        "spark-integration-hub-k8s", app=SPARK_INTEGRATION_HUB, channel="3/edge", trust=True
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
        [
            res
            for res in resources
            if res["kind"] == "Secret"
            and res["metadata"]["name"]
            == f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"
        ]
    )
    assert any(
        [
            res
            for res in resources
            if res["kind"] == "ServiceAccount"
            and res["metadata"]["name"] == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
        ]
    )
    assert any(
        [
            res
            for res in resources
            if res["kind"] == "Role"
            and res["metadata"]["name"] == EXPECTED_ROLE_NAME
        ]
    )
    assert any(
        [
            res
            for res in resources
            if res["kind"] == "RoleBinding"
            and res["metadata"]["name"] == f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role-binding"
        ]
    )


def test_deploy_resource_dispatcher_setup(juju: jubilant.Juju):
    """Deploy the necessary setup for the resource dispatcher."""
    logger.info("Deploying metacontroller-operator charm...")
    juju.deploy(METACONTROLLER_CHARM, channel=METACONTROLLER_CHARM_CHANNEL, trust=True)
    juju.deploy(
        RESOURCE_DISPATCHER,
        channel=RESOURCE_DISPATCHER_CHANNEL,
        trust=True,
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )


def test_secrets_relation(juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str):
    """Integrate the kubeflow integrator with resource dispatcher over the the `secrets` relation and check that the secrets are created."""

    secrets_before_relation = list(lightkube_client.list(Secret, namespace=ALL_NS))
    assert not any(
        [
            secret.metadata
            and secret.metadata.name == f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"
            and secret.metadata.namespace == kubeflow_enabled_namespace
            for secret in secrets_before_relation
        ]
    )

    logger.info(
        "Integrating kubeflow-integrator <> resource-dispatcher over `secrets` endpoint..."
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{SECRETS_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{SECRETS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=SECRETS_RELATION_NAME
            ).values()
        )
    )
    secrets_in_relation_data = json.loads(relation_data["kubernetes_manifests"])
    assert len(secrets_in_relation_data) > 0
    assert secrets_in_relation_data[0]["kind"] == "Secret"
    # assert secrets_in_relation_data[0]["metadata"]["name"] == f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"

    secrets_after_relation = list(lightkube_client.list(Secret, namespace=ALL_NS))
    matching_secrets = [
        secret
        for secret in secrets_after_relation
        if secret.metadata
        and secret.metadata.name == f"integrator-hub-conf-{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}"
        and secret.metadata.namespace == kubeflow_enabled_namespace
    ]

    assert (
        len(matching_secrets) == 1
    ), "Expected exactly one matching secret after relation is created."





def test_service_accounts_relation(juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str):
    """Integrate the kubeflow integrator with resource dispatcher over the the `service-accounts` relation and check that the service accounts are created."""

    serviceaccounts_before_relation = list(lightkube_client.list(ServiceAccount, namespace=ALL_NS))
    assert not any(
        [
            sa.metadata
            and sa.metadata.name == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
            and sa.metadata.namespace == kubeflow_enabled_namespace
            for sa in serviceaccounts_before_relation
        ]
    )

    logger.info(
        "Integrating kubeflow-integrator <> resource-dispatcher over `service-accounts` endpoint..."
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{SERVICE_ACCOUNTS_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{SERVICE_ACCOUNTS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=SERVICE_ACCOUNTS_RELATION_NAME
            ).values()
        )
    )
    serviceaccounts_in_relation_data = json.loads(relation_data["kubernetes_manifests"])
    assert len(serviceaccounts_in_relation_data) > 0
    assert serviceaccounts_in_relation_data[0]["kind"] == "ServiceAccount"
    assert serviceaccounts_in_relation_data[0]["metadata"]["name"] == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE

    serviceaccounts_after_relation = list(lightkube_client.list(ServiceAccount, namespace=ALL_NS))
    matching_serviceaccounts = [
        sa
        for sa in serviceaccounts_after_relation
        if sa.metadata
        and sa.metadata.name == SPARK_SERVICE_ACCOUNT_CONFIG_VALUE
        and sa.metadata.namespace == kubeflow_enabled_namespace
    ]

    assert (
        len(matching_serviceaccounts) == 1
    ), "Expected exactly one matching service account after relation is created."





def test_roles_relation(juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str):
    """Integrate the kubeflow integrator with resource dispatcher over the the `roles` relation and check that the roles are created."""

    roles_before_relation = list(lightkube_client.list(Role, namespace=ALL_NS))
    assert not any(
        [
            role.metadata
            and role.metadata.name == f"{SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}-role"
            and role.metadata.namespace == kubeflow_enabled_namespace
            for role in roles_before_relation
        ]
    )

    logger.info(
        "Integrating kubeflow-integrator <> resource-dispatcher over `roles` endpoint..."
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{ROLES_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{ROLES_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=ROLES_RELATION_NAME
            ).values()
        )
    )
    roles_in_relation_data = json.loads(relation_data["kubernetes_manifests"])
    assert len(roles_in_relation_data) > 0
    assert roles_in_relation_data[0]["kind"] == "Role"
    assert roles_in_relation_data[0]["metadata"]["name"] == EXPECTED_ROLE_NAME

    roles_after_relation = list(lightkube_client.list(Role, namespace=ALL_NS))
    matching_roles = [
        role
        for role in roles_after_relation
        if role.metadata
        and role.metadata.name == EXPECTED_ROLE_NAME
        and role.metadata.namespace == kubeflow_enabled_namespace
    ]

    assert (
        len(matching_roles) == 1
    ), "Expected exactly one matching role after relation is created."



def test_rolebindings_relation(juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str):
    """Integrate the kubeflow integrator with resource dispatcher over the the `role-bindings` relation and check that the rolebindings are created."""

    rolebindings_before_relation = list(lightkube_client.list(RoleBinding, namespace=ALL_NS))
    assert not any(
        [
            rolebinding.metadata
            and rolebinding.metadata.name == EXPECTED_ROLEBINDING_NAME
            and rolebinding.metadata.namespace == kubeflow_enabled_namespace
            for rolebinding in rolebindings_before_relation
        ]
    )

    logger.info(
        "Integrating kubeflow-integrator <> resource-dispatcher over `role-bindings` endpoint..."
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{ROLE_BINDINGS_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{ROLE_BINDINGS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=ROLE_BINDINGS_RELATION_NAME
            ).values()
        )
    )
    rolebindings_in_relation_data = json.loads(relation_data["kubernetes_manifests"])
    assert len(rolebindings_in_relation_data) > 0
    assert rolebindings_in_relation_data[0]["kind"] == "RoleBinding"
    assert rolebindings_in_relation_data[0]["metadata"]["name"] == EXPECTED_ROLEBINDING_NAME

    rolebindings_after_relation = list(lightkube_client.list(RoleBinding, namespace=ALL_NS))
    matching_rolebindings = [
        rolebinding
        for rolebinding in rolebindings_after_relation
        if rolebinding.metadata
        and rolebinding.metadata.name == EXPECTED_ROLEBINDING_NAME
        and rolebinding.metadata.namespace == kubeflow_enabled_namespace
    ]

    assert (
        len(matching_rolebindings) == 1
    ), "Expected exactly one matching rolebindings after relation is created."




def test_poddefaults_relation(juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str):
    """Integrate the kubeflow integrator with resource dispatcher over the the `pod-defaults` relation and check that the poddefaults are created."""

    poddefaults_before_relation = list(lightkube_client.list(POD_DEFAULT, namespace=ALL_NS))
    assert not any(
        [
            poddefault.metadata
            and poddefault.metadata.name in (EXPECTED_NOTEBOOK_PODDEFAULT_NAME, EXPECTED_PIPELINE_PODDEFAULT_NAME)
            and poddefault.metadata.namespace == kubeflow_enabled_namespace
            for poddefault in poddefaults_before_relation
        ]
    )

    logger.info(
        "Integrating kubeflow-integrator <> resource-dispatcher over `pod-defaults` endpoint..."
    )
    juju.integrate(
        f"{KUBEFLOW_INTEGRATOR}:{POD_DEFAULTS_RELATION_NAME}",
        f"{RESOURCE_DISPATCHER}:{POD_DEFAULTS_RELATION_NAME}",
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    relation_data = next(
        iter(
            get_application_data(
                juju=juju, app_name=RESOURCE_DISPATCHER, relation_name=POD_DEFAULTS_RELATION_NAME
            ).values()
        )
    )
    poddefaults_in_relation_data = json.loads(relation_data["kubernetes_manifests"])
    assert len(poddefaults_in_relation_data) == 2

    assert len([
        pd for pd in poddefaults_in_relation_data if pd["kind"] == "PodDefault" and pd["metadata"]["name"] == EXPECTED_NOTEBOOK_PODDEFAULT_NAME
    ]) == 1
    assert len([
        pd for pd in poddefaults_in_relation_data if pd["kind"] == "PodDefault" and pd["metadata"]["name"] == EXPECTED_PIPELINE_PODDEFAULT_NAME
    ]) == 1

    poddefaults_after_relation = list(lightkube_client.list(POD_DEFAULT, namespace=ALL_NS))
    matching_poddefaults = [
        poddefaults
        for poddefaults in poddefaults_after_relation
        if poddefaults.metadata
        and poddefaults.metadata.name in (EXPECTED_NOTEBOOK_PODDEFAULT_NAME, EXPECTED_PIPELINE_PODDEFAULT_NAME)
        and poddefaults.metadata.namespace == kubeflow_enabled_namespace
    ]

    assert (
        len(matching_poddefaults) == 2
    ), "Expected exactly one matching poddefaults after relation is created."



def test_change_in_spark_properties_reflected(juju: jubilant.Juju, lightkube_client: lightkube.Client, kubeflow_enabled_namespace: str):
    """Test that a change in Spark properties by changing an config option integration hub is reflected all the way to the K8s resources."""
    secret_before_changes = lightkube_client.get(Secret, name=EXPECTED_SECRET_NAME, namespace=kubeflow_enabled_namespace)
    spark_properties_before_changes = secret_before_changes.data
    assert spark_properties_before_changes is None

    logger.info("Enabling dynamic pod allocation in Spark Integration Hub...")
    juju.config(
        SPARK_INTEGRATION_HUB,
        {
            "enable-dynamic-allocation": "true"
        }
    )
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    secret_after_changes = lightkube_client.get(Secret, name=EXPECTED_SECRET_NAME, namespace=kubeflow_enabled_namespace)
    spark_properties_after_changes = secret_after_changes.data
    assert spark_properties_after_changes is not None
    decoded_spark_properties = {
        key: base64.b64decode(value).decode("utf-8") for key, value in spark_properties_after_changes.items()
    }
    assert decoded_spark_properties["spark.dynamicAllocation.enabled"] == "true"
    assert decoded_spark_properties["spark.dynamicAllocation.minExecutors"] == "1"
    assert decoded_spark_properties["spark.dynamicAllocation.shuffleTracking.enabled"] == "true"


def test_read_spark_config_using_spark_client(juju: jubilant.Juju, kubeflow_enabled_namespace: str):
    """Test that we can read the spark config using a spark-client snap."""
    logger.info("Reading Spark properties using spark-client snap...")
    process = subprocess.run(
        ["spark-client.service-account-registry", "get-config", "--namespace", kubeflow_enabled_namespace, "--username", SPARK_SERVICE_ACCOUNT_CONFIG_VALUE],
        capture_output=True,
        text=True,  
    )
    assert process.returncode == 0, f"spark-client.service-account-registry get-config command failed with error: {process.stderr}"
    output = process.stdout.strip().splitlines()
    assert "spark.dynamicAllocation.enabled=true" in output
    assert "spark.dynamicAllocation.minExecutors=1" in output 
    assert "spark.dynamicAllocation.shuffleTracking.enabled=true" in output


def test_sleep():
    """A dummy test to introduce a sleep time for manual verification."""
    import time
    logger.info("Sleeping for 300 seconds to allow manual verification if needed...")
    time.sleep(300)

# def test_run_spark_job_using_spark_client(juju: jubilant.Juju, kubeflow_enabled_namespace: str):
#     """Test that we can run a spark job using the spark-client snap and the created service account."""
    
#     # pods_before_spark_job = lightkube_client.list(
#     logger.info("Running Spark job using spark-client snap...")


#     process = subprocess.run(
#         [
#             "spark-client.spark-submit",
#             "--namespace", kubeflow_enabled_namespace,
#             "--username", SPARK_SERVICE_ACCOUNT_CONFIG_VALUE,
#             "--class", "org.apache.spark.examples.SparkPi",
#             # "--master", "k8s://https://kubernetes.default.svc",
#             "--deploy-mode", "cluster",
#             # "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={SPARK_SERVICE_ACCOUNT_CONFIG_VALUE}",
#             # "--conf", "spark.executor.instances=2",
#             # "--conf", "spark.kubernetes.container.image=spark-client/spark:3.5.0",
#             "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar",
#             "1000"
#         ],
#         capture_output=True,
#         text=True,
#     )
#     assert process.returncode == 0, f"spark-client.spark-submit command failed with error: {process.stderr}"
#     assert "Pi is roughly 3.14" in process.stdout





# # somewhere
# # test that change in integration-hub config option does change
# # all the way until the K8s resource.












# # def test_integrate_kubeflow_with_resource_dispatcher(
# #     juju: jubilant.Juju, microk8s_model: str, kubeflow_integrator: str
# # ):
# #     """Tesing the integration between kubeflow-integration and resource disptacher.

# #     Test deploying the kubeflow-integrator charm and resource dispatcher, integrate
# #     them and validate all active.
# #     """
# #     # save the temp_model
# #     temp_model = juju.model
# #     # Switch to the k8s model
# #     juju.model = microk8s_model
# #     # Install resource dispatcher and its dependencies
# #     juju.deploy(METACONTROLLER_CHARM, trust=True)
# #     juju.deploy(ADMISSION_WEBHOOK_CHARM, trust=True)
# #     juju.deploy(
# #         RESOURCE_DISPATCHER,
# #         trust=True,
# #         channel=RESOURCE_DISPATCHER_CHANNEL,
# #     )

# #     # Wait for charms to be active
# #     status = juju.wait(
# #         lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
# #     )

# #     logger.info(status)

# #     # Offer interfaces
# #     logger.info("Offering resource dispatcher interfaces")
# #     juju.offer(
# #         f"{microk8s_model}.{RESOURCE_DISPATCHER_APP_NAME}",
# #         name="secrets-dispatcher",
# #         endpoint="secrets",
# #     )
# #     juju.offer(
# #         f"{microk8s_model}.{RESOURCE_DISPATCHER_APP_NAME}",
# #         name="poddefaults-dispatcher",
# #         endpoint="pod-defaults",
# #     )

# #     # Switch to the lxd model
# #     logger.info("Switching to kubeflow-integrator model")
# #     juju.model = temp_model

# #     juju.deploy(
# #         kubeflow_integrator,
# #         app=KUBEFLOW_INTEGRATOR_APP_NAME,
# #         config={"profile": "profile-name"},
# #     )

# #     # Consume offers
# #     logger.info("Consuming offers")
# #     juju.consume(f"{microk8s_model}.secrets-dispatcher")
# #     juju.consume(f"{microk8s_model}.poddefaults-dispatcher")

# #     logger.info("Waiting for all units to become active")
# #     juju.wait(
# #         lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
# #     )

# #     # Integrate kubeflow-integrator with resource-dispatcher
# #     logger.info("Integrate kubeflow-integrator with resource-dispatcher")
# #     juju.integrate(f"{KUBEFLOW_INTEGRATOR_APP_NAME}:secrets", "secrets-dispatcher")
# #     juju.integrate(f"{KUBEFLOW_INTEGRATOR_APP_NAME}:pod-defaults", "poddefaults-dispatcher")

# #     # Wait for the relatio data to be created

# #     # Switch to k8s model
# #     juju.model = microk8s_model

# #     logger.info("Checking secrets and pod defaults relation data")

# #     for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(3), reraise=True):
# #         with attempt:
# #             # Check application data
# #             secrets_rel_data = list(
# #                 get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "secrets").items()
# #             )[0]
# #             assert secrets_rel_data[1] == {}
# #             assert "kubernetes_manifests" not in secrets_rel_data[1]
# #             poddefaults_rel_data = list(
# #                 get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "pod-defaults").items()
# #             )[0]
# #             assert poddefaults_rel_data[1] == {}
# #             assert "kubernetes_manifests" not in poddefaults_rel_data[1]

# #     juju.model = temp_model


# # def test_manifests_generation_with_opensearch(juju: jubilant.Juju, microk8s_model: str):
# #     """Tesing Manifests Generation when related to OpenSearch.

# #     Deploy opensearch, integrate it with kubeflow-integrator and make sure that
# #     manifests are generated in the relation data.
# #     """
# #     temp_model = juju.model
# #     # Deploy opensearch and self-signed-certificates
# #     logger.info("Deploying OpenSearch charm")
# #     juju.config(KUBEFLOW_INTEGRATOR_APP_NAME, {"opensearch-index-name": "index-name"})

# #     juju.deploy(
# #         OPENSEARCH_CHARM,
# #         app=OPENSEARCH_APP_NAME,
# #         channel=OPENSEARCH_CHANNEL,
# #         config={"profile": OPENSEARCH_PROFILE_CONFIG},
# #     )

# #     logger.info("Deploying self-signed-certificates charm")
# #     juju.deploy(
# #         SELF_SIGNED_CERTIFICATES_CHARM,
# #         app=SELF_SIGNED_CERTIFICATES_APP_NAME,
# #         channel=SELF_SIGNED_CERTIFICATES_CHANNEL,
# #     )
# #     logger.info("Integrate opensearch with self-signed-certificates")
# #     # Integrate opensearch with self-signed-certificates
# #     juju.integrate(OPENSEARCH_APP_NAME, SELF_SIGNED_CERTIFICATES_APP_NAME)

# #     logger.info("Waiting for opensearch to be active")
# #     juju.wait(
# #         lambda status: jubilant.all_active(status, OPENSEARCH_APP_NAME)
# #         and jubilant.all_agents_idle(status, OPENSEARCH_APP_NAME),
# #         delay=5,
# #         timeout=600,
# #     )

# #     logger.info("Integrate opensearch with kubeflow-integrator")
# #     juju.integrate(OPENSEARCH_APP_NAME, KUBEFLOW_INTEGRATOR_APP_NAME)

# #     logger.info("Waiting for kubeflow-integrator to be active")
# #     juju.wait(
# #         lambda status: jubilant.all_active(status, KUBEFLOW_INTEGRATOR_APP_NAME)
# #         and jubilant.all_agents_idle(status, KUBEFLOW_INTEGRATOR_APP_NAME),
# #         delay=5,
# #         timeout=600,
# #     )

# #     # Switch to k8s model
# #     juju.model = microk8s_model

# #     # Check application data
# #     secrets_rel_data = list(
# #         get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "secrets").items()
# #     )[0]
# #     assert secrets_rel_data[1] != {}
# #     poddefaults_rel_data = list(
# #         get_application_data(juju, RESOURCE_DISPATCHER_APP_NAME, "pod-defaults").items()
# #     )[0]
# #     assert poddefaults_rel_data[1] != {}

# #     assert "kubernetes_manifests" in secrets_rel_data[1]
# #     secrets_k8s_manifests = json.loads(secrets_rel_data[1]["kubernetes_manifests"])
# #     for secret_manifest in secrets_k8s_manifests:
# #         secret = validate_k8s_secret(secret_manifest)
# #         if secret.metadata.name == "opensearch-secret":
# #             assert secret.data["OPENSEARCH_INDEX"] == base64.b64encode(b"index-name").decode()

# #     assert "kubernetes_manifests" in poddefaults_rel_data[1]
# #     poddefaults_k8s_manifests = json.loads(poddefaults_rel_data[1]["kubernetes_manifests"])
# #     for poddefault_manifest in poddefaults_k8s_manifests:
# #         validate_k8s_poddefault(poddefault_manifest)

# #     juju.model = temp_model
