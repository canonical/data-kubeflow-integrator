#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

"""Charm constants."""

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


K8S_OPENSEARCH_SECRET_NAME = "opensearch-secret"
K8S_OPENSEARCH_PODDEFAULT_NAME = "opensearch-pod-default"
K8S_OPENSEARCH_PODDEFAULT_DESC = "OpenSearch Credentials"
K8S_OPENSEARCH_PODDEFAULT_SELECTOR_LABEL = "access-opensearch"
