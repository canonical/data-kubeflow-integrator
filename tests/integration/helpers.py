#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging

import jubilant
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


def get_application_data(juju: jubilant.Juju, app_name: str, relation_name: str) -> dict:
    """Retrieves the application data from a specific relation.

    Args:
        juju: The Juju client object used to execute CLI commands.
        app_name: The name of the Juju application.
        relation_name: The name of the relation endpoint to query.

    Returns:
        A dictionary containing the application data for the specified relation.

    Raises:
        ValueError: If no relation data can be found for the specified
            relation endpoint.
    """
    unit_name = f"{app_name}/0"
    command_stdout = juju.cli("show-unit", unit_name, "--format=json")
    result = json.loads(command_stdout)

    relation_data = [
        v for v in result[unit_name]["relation-info"] if v["endpoint"] == relation_name
    ]

    if len(relation_data) == 0:
        raise ValueError(
            f"No relation data could be grabbed on relation with endpoint {relation_name}"
        )

    return {relation["relation-id"]: relation["application-data"] for relation in relation_data}


OPENSEARCH_MODEL_CONFIG = {
    "logging-config": "<root>=INFO;unit=DEBUG",
    "cloudinit-userdata": """postruncmd:
        - [ 'sysctl', '-w', 'vm.max_map_count=262144' ]
        - [ 'sysctl', '-w', 'fs.file-max=1048576' ]
        - [ 'sysctl', '-w', 'vm.swappiness=0' ]
        - [ 'sysctl', '-w', 'net.ipv4.tcp_retries2=5' ]
    """,
}


class K8sMetadata(BaseModel):
    """Kubernetes metadata section."""

    name: str
    namespace: str | None


class K8sSecret(BaseModel):
    """Kubernetes secret manifest model."""

    api_version: str = Field("v1", alias="apiVersion")
    kind: str = Field("Secret")
    metadata: K8sMetadata
    string_data: dict[str, str] = Field(alias="stringData")


class EnvValueFromSecret(BaseModel):
    """Kubernetes environment variable from secret."""

    name: str
    key: str
    optional: bool = Field(False)


class EnvValueFrom(BaseModel):
    """envValueFrom model."""

    secret_key_ref: EnvValueFromSecret = Field(alias="secretKeyRef")


class K8sEnv(BaseModel):
    """Environment variable model."""

    name: str
    value_from: EnvValueFrom = Field(alias="valueFrom")


class K8sPodDefault(BaseModel):
    """Kubernetes model for pod default."""

    class K8sPodDefaultSpec(BaseModel):
        """pod default spec section."""

        env: list[K8sEnv]

    api_version: str = Field("kubeflow.org/v1alpha1", alias="apiVersion")
    kind: str = Field("PodDefault")
    metadata: K8sMetadata
    spec: K8sPodDefaultSpec


def validate_k8s_secret(
    manifest: dict, keys_values_to_check: dict[str, str] | None = None
) -> bool:
    """Validate that the manifest is a kubernetes secret manifest."""
    try:
        secret = K8sSecret(**manifest)
        if keys_values_to_check:
            for key, value in keys_values_to_check.items():
                assert secret.string_data[key] == value
        return True
    except ValidationError as e:
        logger.error("Validation Error of kubernetes secret")
        logger.error(e)
        return False


def validate_k8s_poddefault(manifest: dict) -> bool:
    """Validate that the manifest is a kubernetes pod default manifest."""
    try:
        K8sPodDefault(**manifest)
        return True
    except ValidationError as e:
        logger.error("Validation Error of kubernetes pod default")
        logger.error(e)
        return False
