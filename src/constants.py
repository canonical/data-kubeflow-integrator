#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

"""Charm constants."""

POSTGRESQL = "postgresql"
MONGODB = "mongodb"
MYSQL = "mysql"
OPENSEARCH = "opensearch"
KAFKA = "kafka"
SPARK = "spark"
KFP_S3_STORAGE = "kfp-s3-storage"


OPENSEARCH_RELATION_NAME = "opensearch"
POSTGRESQL_RLEATION_NAME = "postgresql"
MYSQL_RELATION_NAME = "mysql"
MONGODB_RELATION_NAME = "mongodb"
KAFKA_RELATION_NAME = "kafka"
SPARK_RELATION_NAME = "spark"
KFP_S3_STORAGE_RELATION_NAME = "kfp-s3-storage"

STATUS_PEERS_RELATION_NAME = "status-peers"
SECRETS_DISPATCHER_RELATION_NAME = "secrets"
SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME = "service-accounts"
POD_DEFAULTS_DISPATCHER_RELATION_NAME = "pod-defaults"
ROLES_DISPATCHER_RELATION_NAME = "roles"
ROLEBINDINGS_DISPATCHER_RELATION_NAME = "role-bindings"
CONFIGMAPS_DISPATCHER_RELATION_NAME = "config-maps"
PEER_RELATION = "kubeflow-integrator-peers"


# Kubeflow S3 / MinIO artifact store resources (for multi-tenancy pipelines)
MLPIPELINE_MINIO_ARTIFACT_SECRET_NAME = "mlpipeline-minio-artifact"
ARTIFACT_REPOSITORIES_CONFIGMAP_NAME = "artifact-repositories"
KFP_LAUNCHER_CONFIGMAP_NAME = "kfp-launcher"

# Keys under which the MinIO credentials are stored in the artifact secret
MINIO_SECRET_ACCESS_KEY = "accesskey"
MINIO_SECRET_SECRET_KEY = "secretkey"

# The AWS S3 SDK used by the kfp-launcher requires a non-empty region, even when talking to a
# local storage that has no region concept. Use the same default as used in
# `files/upstream/sync.py` of kfp-profile-controller, see:
# https://github.com/canonical/kfp-operators/tree/dd3b8d1bfefa7322fba900ae7a6c797d1890d961/charms/kfp-profile-controller
DEFAULT_REGION = "us-east-1"

# argo `artifact-repositories` config-map annotation marking the default repository
ARTIFACT_REPOSITORY_ANNOTATION = "workflows.argoproj.io/default-artifact-repository"
ARTIFACT_REPOSITORY_REF = "default-namespaced"

# argo `keyFormat` for the artifact-repositories config-map. Using %raw% tags since
# resource-dispatcher renders every manifest it receives through Jinja2.
ARTIFACT_KEY_FORMAT = (
    "{%raw%}"
    "artifacts/{{workflow.name}}/{{workflow.creationTimestamp.Y}}/"
    "{{workflow.creationTimestamp.m}}/{{workflow.creationTimestamp.d}}/{{pod.name}}"
    "{%endraw%}"
)

# Default template used for the `kfp-launcher` defaultPipelineRoot when unset via config
DEFAULT_PIPELINE_ROOT_TEMPLATE = "minio://{bucket}/v2/artifacts"

# Mandatory fields an S3 provider must advertise over the kfp-s3-storage relation
S3_REQUIRED_FIELDS = ("access-key", "secret-key", "bucket", "endpoint")

K8S_TLS_SECRET_VOLUME = "tls-secret"
K8S_TLS_MOUNTPATH = "/etc/data-kubeflow-integrator"


K8S_DATABASE_SECRET_NAME = {
    POSTGRESQL: "postgresql-secret",
    MONGODB: "mongodb-secret",
    MYSQL: "mysql-secret",
    OPENSEARCH: "opensearch-secret",
    KAFKA: "kafka-secret",
}

K8S_DATABASE_TLS_SECRET_NAME = {
    POSTGRESQL: "postgresql-tls-secret",
    MONGODB: "mongodb-tls-secret",
    MYSQL: "mysql-tls-secret",
    OPENSEARCH: "opensearch-tls-secret",
    KAFKA: "kafka-tls-secret",
}

K8S_DATABASE_PODDEFAULT_NAME = {
    POSTGRESQL: "postgresql-pod-default",
    MONGODB: "mongodb-pod-default",
    MYSQL: "mysql-pod-default",
    OPENSEARCH: "opensearch-pod-default",
    KAFKA: "kafka-pod-default",
}
SPARK_PIPELINE_PODDEFAULT_NAME = "pyspark-pipeline"
SPARK_NOTEBOOK_PODDEFAULT_NAME = "pyspark-notebook"

K8S_DATABASE_PODDEFAULT_DESC = {
    POSTGRESQL: "Postgresql Credentials",
    MONGODB: "MongoDB Credentials",
    MYSQL: "Mysql Credentials",
    OPENSEARCH: "OpenSearch Credentials",
    KAFKA: "Kafka Credentials",
}
SPARK_PIPELINE_PODDEFAULT_DESC = "Configure PySpark for Kubeflow pipelines"
SPARK_NOTEBOOK_PODDEFAULT_DESC = "Configure PySpark for Kubeflow notebooks"

K8S_DATABASE_PODDEFAULT_SELECTOR_LABEL = {
    POSTGRESQL: "access-postgresql",
    MONGODB: "access-mongodb",
    MYSQL: "access-mysql",
    OPENSEARCH: "access-opensearch",
    KAFKA: "access-kafka",
    SPARK: "access-spark",
}
SPARK_NOTEBOOK_PODDEFAULT_SELECTOR_LABEL = "access-spark-notebook"
SPARK_PIPELINE_PODDEFAULT_SELECTOR_LABEL = "access-spark-pipeline"


K8S_DATABASE_TLS_CERT_PATH = {
    POSTGRESQL: f"{K8S_TLS_MOUNTPATH}/{POSTGRESQL}_ca.crt",
    MONGODB: f"{K8S_TLS_MOUNTPATH}/{MONGODB}_ca.crt",
    MYSQL: f"{K8S_TLS_MOUNTPATH}/{MYSQL}_ca.crt",
    OPENSEARCH: f"{K8S_TLS_MOUNTPATH}/{OPENSEARCH}_ca.crt",
    KAFKA: f"{K8S_TLS_MOUNTPATH}/{KAFKA}_ca.crt",
}


# Spark related configs
SPARK_DRIVER_PORT = 37371
SPARK_BLOCK_MANAGER_PORT = 6060
