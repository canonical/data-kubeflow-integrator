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


OPENSEARCH_RELATION_NAME = "opensearch"
POSTGRESQL_RLEATION_NAME = "postgresql"
MYSQL_RELATION_NAME = "mysql"
MONGODB_RELATION_NAME = "mongodb"
KAFKA_RELATION_NAME = "kafka"
SPARK_RELATION_NAME = "spark"

STATUS_PEERS_RELATION_NAME = "status-peers"
SECRETS_DISPATCHER_RELATION_NAME = "secrets"
SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME = "service-accounts"
POD_DEFAULTS_DISPATCHER_RELATION_NAME = "pod-defaults"
ROLES_DISPATCHER_RELATION_NAME = "roles"
ROLEBINDINGS_DISPATCHER_RELATION_NAME = "role-bindings"
PEER_RELATION = "kubeflow-integrator-peers"

K8S_TLS_SECRET_VOLUME = "tls-secret"
K8S_TLS_MOUNTPATH = "/etc/data-kubeflow-integrator"


K8S_DATABASE_SECRET_NAME = {
    POSTGRESQL: "postgresql-secret",
    MONGODB: "mongodb-secret",
    MYSQL: "mysql-secret",
    OPENSEARCH: "opensearch-secret",
    KAFKA: "kafka-secret",
    SPARK: "spark-secret",
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
    SPARK: "spark-pod-default",
}
SPARK_PIPELINE_PODDEFAULT_NAME = "pyspark-pipeline"
SPARK_NOTEBOOK_PODDEFAULT_NAME = "pyspark-notebook"

K8S_DATABASE_PODDEFAULT_DESC = {
    POSTGRESQL: "Postgresql Credentials",
    MONGODB: "MongoDB Credentials",
    MYSQL: "Mysql Credentials",
    OPENSEARCH: "OpenSearch Credentials",
    KAFKA: "Kafka Credentials",
    SPARK: "Spark Service Account",
}
SPARK_PIPELINE_PODDEFAULT_DESC = "Configure PySpark for Kubeflow pipelines"
SPARK_NOTEBOOK_PODDEFAULT_DESC = "pyspark-notebook"

K8S_DATABASE_PODDEFAULT_SELECTOR_LABEL = {
    POSTGRESQL: "access-postgresql",
    MONGODB: "access-mongodb",
    MYSQL: "access-mysql",
    OPENSEARCH: "access-opensearch",
    KAFKA: "access-kafka",
    SPARK: "access-spark",
}


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
SPARK_IMAGE = "ghcr.io/canonical/charmed-spark@sha256:1d9949dc7266d814e6483f8d9ffafeff32f66bb9939e0ab29ccfd9d5003a583a"
