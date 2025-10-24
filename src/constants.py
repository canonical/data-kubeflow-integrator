#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

"""Charm constants."""

POSTGRESQL = "postgresql"
MONGODB = "mongodb"
MYSQL = "mysql"
OPENSEARCH = "opensearch"
KAFKA = "kafka"


OPENSEARCH_RELATION_NAME = "opensearch"
POSTGRESQL_RLEATION_NAME = "postgresql"
MYSQL_RELATION_NAME = "mysql"
MONGODB_RELATION_NAME = "mongodb"
KAFKA_RELATION_NAME = "kafka"
STATUS_PEERS_RELATION_NAME = "status-peers"
SECRETS_DISPATCHER_RELATION_NAME = "secrets"
SERVICE_ACCOUNTS_DISPATCHER_RELATION_NAME = "service-accounts"
POD_DEFAULTS_DISPATCHER_RELATION_NAME = "pod-defaults"
PEER_RELATION = "kubeflow-integrator-peers"

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

K8S_DATABASE_PODDEFAULT_DESC = {
    POSTGRESQL: "Postgresql Credentials",
    MONGODB: "MongoDB Credentials",
    MYSQL: "Mysql Credentials",
    OPENSEARCH: "OpenSearch Credentials",
    KAFKA: "Kafka Credentials",
}

K8S_DATABASE_PODDEFAULT_SELECTOR_LABEL = {
    POSTGRESQL: "access-postgresql",
    MONGODB: "access-mongodb",
    MYSQL: "access-mysql",
    OPENSEARCH: "access-opensearch",
    KAFKA: "access-kafka",
}
