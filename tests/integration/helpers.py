#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import tempfile

import jubilant
import requests
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)

OPENSEARCH_MODEL_CONFIG = {
    "logging-config": "<root>=INFO;unit=DEBUG",
    "cloudinit-userdata": """postruncmd:
        - [ 'sysctl', '-w', 'vm.max_map_count=262144' ]
        - [ 'sysctl', '-w', 'fs.file-max=1048576' ]
        - [ 'sysctl', '-w', 'vm.swappiness=0' ]
        - [ 'sysctl', '-w', 'net.ipv4.tcp_retries2=5' ]
    """,
}


def send_request(
    method: str,
    endpoints: str,
    http_path: str,
    credentials: dict[str, str],
    payload: str | None = None,
) -> requests.Response:
    """Sending HTTP request to endpoint with credentials and payloads."""
    username = credentials["username"]
    password = credentials["password"]
    servers = endpoints.split(",")

    if not (username and password and servers):
        raise KeyError("missing credentials or endpoints")

    if http_path.startswith("/"):
        http_path = http_path[1:]

    full_url = f"https://{servers[0]}/{http_path}"

    with requests.Session() as s, tempfile.NamedTemporaryFile(mode="w+") as chain:
        chain.write(credentials.get("tls-ca"))
        chain.seek(0)
        request_kwargs = {
            "verify": chain.name,
            "method": method.upper(),
            "url": full_url,
            "headers": {"Content-Type": "application/json", "Accept": "application/json"},
        }
        if payload:
            request_kwargs["data"] = payload

        s.auth = (username, password)
        resp = s.request(**request_kwargs)
    try:
        return resp
    except json.JSONDecodeError:
        return resp


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


class K8sMetadata(BaseModel):
    """Kubernetes metadata section."""

    name: str
    namespace: str | None


class K8sSecret(BaseModel):
    """Kubernetes secret manifest model."""

    api_version: str = Field("v1", alias="apiVersion")
    kind: str = Field("Secret")
    metadata: K8sMetadata
    data: dict[str, str] = Field(alias="data")


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


def validate_k8s_secret(manifest: dict) -> K8sSecret:
    """Validate that the manifest is a kubernetes secret manifest."""
    try:
        secret = K8sSecret(**manifest)
        return secret
    except ValidationError as e:
        logger.error("Validation Error of kubernetes secret")
        logger.error(e)
        raise e


def validate_k8s_poddefault(manifest: dict) -> bool:
    """Validate that the manifest is a kubernetes pod default manifest."""
    try:
        K8sPodDefault(**manifest)
        return True
    except ValidationError as e:
        logger.error("Validation Error of kubernetes pod default")
        logger.error(e)
        return False
