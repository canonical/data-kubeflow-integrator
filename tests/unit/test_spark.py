#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit test module for data-kubeflow-integrator integration with Spark."""

import dataclasses
from pathlib import Path

import yaml
from charms.data_platform_libs.v0.data_models import json
from ops import ActiveStatus, BlockedStatus, testing
from ops.testing import Relation, State

from charm import KubeflowIntegratorCharm

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())

SPARK_PROPERTIES_DATA = '{"spark.dynamicAllocation.enabled": "true"}'
RESOURCE_MANIFEST_DATA = """
apiVersion: v1
kind: ServiceAccount
metadata:
  labels:
    app.kubernetes.io/generated-by: spark8t
  name: spark
  namespace: profile-name
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  labels:
    app.kubernetes.io/generated-by: spark8t
  name: spark-role
  namespace: profile-name
rules:
- apiGroups:
  - ''
  resources:
    - pods
    - configmaps
    - services
    - serviceaccounts
  verbs:
    - create
    - get
    - list
    - watch
    - delete
    - deletecollection
    - update
    - patch
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  labels:
    app.kubernetes.io/generated-by: spark8t
  name: spark-role-binding
  namespace: profile-name
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-role
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: profile-name
---
apiVersion: v1
kind: Secret
metadata:
  labels:
    app.kubernetes.io/generated-by: integration-hub
  name: integrator-hub-conf-spark
  namespace: profile-name
stringData:
  spark.dynamicAllocation.enabled: 'true'
"""


def test_charm_blocked_when_integrated_with_spark_with_no_serviceaccount(
    base_state: State, charm_configuration: dict
):
    """Check that charm will be in a blocked status when integrated with Spark and spark-service-account option not set."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    state_in = base_state
    state_out = ctx.run(ctx.on.start(), state_in)
    assert state_out.unit_status == ActiveStatus()

    spark_relation = Relation(endpoint="spark")
    relations = [*state_out.relations, spark_relation]
    state_in = dataclasses.replace(state_out, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(spark_relation), state_in)
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing config(s): 'spark-service-account'" in status.message


def test_charm_active_and_manifests_generated_when_serviceaccount_configured(
    base_state, charm_configuration: dict
):
    """Check that charm will be in active status and sharing manifests when serviceaccount is configured."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    charm_configuration["options"]["spark-service-account"]["default"] = "spark"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    spark_relation = Relation(
        endpoint="spark",
        remote_app_data={
            "service-account": "profile-name:spark",
            "spark-properties": SPARK_PROPERTIES_DATA,
            "resource-manifest": RESOURCE_MANIFEST_DATA,
        },
    )
    relations = [*base_state.relations, spark_relation]
    state_in = dataclasses.replace(base_state, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(spark_relation), state_in)
    assert state_out.app_status == ActiveStatus()

    secrets_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    service_accounts_relation = Relation(
        endpoint="service-accounts", interface="kubernetes_manifest"
    )
    roles_relation = Relation(endpoint="roles", interface="kubernetes_manifest")
    role_bindings_relation = Relation(endpoint="role-bindings", interface="kubernetes_manifest")
    pod_defaults_relation = Relation(endpoint="pod-defaults", interface="kubernetes_manifest")
    relations = [
        *state_out.relations,
        secrets_relation,
        service_accounts_relation,
        roles_relation,
        role_bindings_relation,
        pod_defaults_relation,
    ]
    state_in = dataclasses.replace(state_out, relations=relations)
    state_out = ctx.run(ctx.on.relation_changed(secrets_relation), state_in)
    assert state_out.app_status == ActiveStatus()

    # Make sure the secret manifest is generated
    kubernetes_manifests = state_out.get_relation(secrets_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    generated_secret = kubernetes_manifests[0]
    assert generated_secret["apiVersion"] == "v1"
    assert generated_secret["kind"] == "Secret"
    assert generated_secret["metadata"]["name"] == "integrator-hub-conf-spark"

    # Make sure the service-accounts manifest is generated
    kubernetes_manifests = state_out.get_relation(service_accounts_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    service_account = kubernetes_manifests[0]
    assert service_account["apiVersion"] == "v1"
    assert service_account["kind"] == "ServiceAccount"
    assert service_account["metadata"]["name"] == "spark"

    # Make sure the roles manifest is generated
    kubernetes_manifests = state_out.get_relation(roles_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    role = kubernetes_manifests[0]
    assert role["apiVersion"] == "rbac.authorization.k8s.io/v1"
    assert role["kind"] == "Role"
    assert role["metadata"]["name"] == "spark-role"

    # Make sure the role-bindings manifest is generated
    kubernetes_manifests = state_out.get_relation(role_bindings_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    rolebinding = kubernetes_manifests[0]
    assert rolebinding["apiVersion"] == "rbac.authorization.k8s.io/v1"
    assert rolebinding["kind"] == "RoleBinding"
    assert rolebinding["metadata"]["name"] == "spark-role-binding"

    # Make sure the pod-defaults manifest is generated
    kubernetes_manifests = state_out.get_relation(pod_defaults_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    assert len(kubernetes_manifests) == 2

    pipeline_poddefault = [
        pd for pd in kubernetes_manifests if pd["metadata"]["name"] == "pyspark-pipeline"
    ][0]
    notebook_poddefault = [
        pd for pd in kubernetes_manifests if pd["metadata"]["name"] == "pyspark-notebook"
    ][0]

    assert pipeline_poddefault["apiVersion"] == "kubeflow.org/v1alpha1"
    assert pipeline_poddefault["kind"] == "PodDefault"
    assert pipeline_poddefault["metadata"]["namespace"] == "profile-name"
    env_vars = {
        obj["name"]: (obj.get("value") or obj.get("valueFrom"))
        for obj in pipeline_poddefault["spec"]["env"]
    }
    assert env_vars["SPARK_SERVICE_ACCOUNT"] == "spark"
    assert env_vars["SPARK_NAMESPACE"] == {"fieldRef": {"fieldPath": "metadata.namespace"}}
    assert pipeline_poddefault["spec"]["selector"] == {
        "matchLabels": {"access-spark-pipeline": "true"}
    }

    assert notebook_poddefault["apiVersion"] == "kubeflow.org/v1alpha1"
    assert notebook_poddefault["kind"] == "PodDefault"
    assert notebook_poddefault["metadata"]["namespace"] == "profile-name"
    env_vars = {
        obj["name"]: (obj.get("value") or obj.get("valueFrom"))
        for obj in notebook_poddefault["spec"]["env"]
    }
    assert env_vars["SPARK_SERVICE_ACCOUNT"] == "spark"
    assert env_vars["SPARK_NAMESPACE"] == {"fieldRef": {"fieldPath": "metadata.namespace"}}
    assert notebook_poddefault["spec"]["selector"] == {
        "matchLabels": {"access-spark-notebook": "true"}
    }
