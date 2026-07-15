#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helper methods for kubernetes manifests generation."""

import base64
from urllib.parse import urlparse

from charms.resource_dispatcher.v0.kubernetes_manifests import KubernetesManifest
from jinja2 import Template

from constants import (
    ARTIFACT_KEY_FORMAT,
    ARTIFACT_REPOSITORIES_CONFIGMAP_NAME,
    ARTIFACT_REPOSITORY_ANNOTATION,
    ARTIFACT_REPOSITORY_REF,
    K8S_DATABASE_PODDEFAULT_DESC,
    K8S_DATABASE_PODDEFAULT_NAME,
    K8S_DATABASE_PODDEFAULT_SELECTOR_LABEL,
    K8S_DATABASE_SECRET_NAME,
    K8S_DATABASE_TLS_SECRET_NAME,
    K8S_TLS_MOUNTPATH,
    K8S_TLS_SECRET_VOLUME,
    KFP_LAUNCHER_CONFIGMAP_NAME,
    MINIO_SECRET_ACCESS_KEY,
    MINIO_SECRET_SECRET_KEY,
    MLPIPELINE_MINIO_ARTIFACT_SECRET_NAME,
)
from utils.k8s_models import (
    EnvVarFromField,
    EnvVarFromSecret,
    K8sPodDefaultManifestInfo,
    K8sSecretManifestInfo,
    PodDefaultAnnotation,
    PodDefaultEnvVar,
    PodDefaultSecretVolume,
)


def format_credentials_data(data: dict[str, str], database_name: str) -> dict[str, str]:
    """Format secret credential data to include database name and make keys uppercase."""
    return {
        f"{database_name.upper()}_{key.upper()}": base64.b64encode(value.encode()).decode("utf-8")
        for key, value in data.items()
        if value
    }


def generate_tls_secret_manifest(
    template: Template, profile: str, creds: dict[str, str], database_name: str
) -> KubernetesManifest | None:
    """Generate TLS secret kubernetes manifest."""
    # Generate a separate secret for tls since this will be mounted as a volume
    # kafka charm sets tls-ca to disabled
    if creds.get("tls-ca") and creds["tls-ca"] != "disabled":
        tls_data = {
            f"{database_name}_ca.crt": base64.b64encode(creds["tls-ca"].encode()).decode("utf-8")
        }
        k8s_tls_secret_info = K8sSecretManifestInfo(
            name=K8S_DATABASE_TLS_SECRET_NAME[database_name],
            namespace=None if profile == "*" else profile,
            data=tls_data,
            labels=None,
        )
        rendered = template.render(secret=k8s_tls_secret_info)
        return KubernetesManifest(rendered)
    return None


def generate_secret_manifest(
    template: Template, profile, creds: dict[str, str], database_name: str
) -> KubernetesManifest:
    """Generate Database Kubernetes secret manifest."""
    # format the secret data
    secret_data = format_credentials_data(creds, database_name)

    k8s_secret_info = K8sSecretManifestInfo(
        name=K8S_DATABASE_SECRET_NAME[database_name],
        namespace=None if profile == "*" else profile,
        data=secret_data,
        labels=None,
    )
    # generate using jinja
    rendered = template.render(secret=k8s_secret_info)
    return KubernetesManifest(rendered)


def generate_poddefault_manifest(
    template: Template,
    profile: str,
    creds: dict[str, str],
    database_name: str,
    from_secret: str | None = None,
    tls_secret: str | None = None,
    poddefault_name: str | None = None,
    poddefault_description: str | None = None,
    annotations: dict[str, str] | None = None,
    args: list[str] | None = None,
    fieldrefs: dict[str, str] | None = None,
    selector_name: str | None = None,
):
    """Generate PodDefault manifest for a database."""
    poddefault_secret_volumes: list[PodDefaultSecretVolume] | None = None
    poddefault_annotations: list[PodDefaultAnnotation] | None = None
    poddefault_env_vars = []
    if tls_secret:
        # TLS should be stored in a mounted volume
        poddefault_secret_volumes = [
            PodDefaultSecretVolume(
                name=K8S_TLS_SECRET_VOLUME,
                secret_name=tls_secret,
                mount_path=K8S_TLS_MOUNTPATH,
            )
        ]
    if from_secret:
        formatted_data = format_credentials_data(creds, database_name)
        poddefault_env_vars += [
            PodDefaultEnvVar(
                name=key,
                secret=EnvVarFromSecret(secret_name=from_secret, secret_key=key),
            )
            for key, _ in formatted_data.items()
        ]
    else:
        if creds.get("tls-ca"):
            creds["tls-ca"] = creds["tls-ca"].replace("\n", "")
        poddefault_env_vars += [
            PodDefaultEnvVar(name=key, value=value) for key, value in creds.items()
        ]
    if fieldrefs:
        poddefault_env_vars += [
            PodDefaultEnvVar(name=key, fieldref=EnvVarFromField(field_path=value))
            for key, value in fieldrefs.items()
        ]
    if annotations:
        poddefault_annotations = [
            PodDefaultAnnotation(key=key, value=value) for key, value in annotations.items()
        ]

    k8s_poddefault_info = K8sPodDefaultManifestInfo(
        name=poddefault_name or K8S_DATABASE_PODDEFAULT_NAME[database_name],
        namespace=None if profile == "*" else profile,
        desc=poddefault_description or K8S_DATABASE_PODDEFAULT_DESC[database_name],
        selector_name=selector_name or K8S_DATABASE_PODDEFAULT_SELECTOR_LABEL[database_name],
        env_vars=poddefault_env_vars,
        secret_volumes=poddefault_secret_volumes,
        annotations=poddefault_annotations,
        args=args,
    )

    rendered = template.render(pod_default=k8s_poddefault_info)
    return KubernetesManifest(rendered)


def _parse_s3_endpoint(endpoint: str) -> tuple[str, bool]:
    """Parse an S3 endpoint into a ``(host[:port], secure)`` tuple.

    The endpoint may be a full URL (e.g. ``https://s3.example.com:443``) or a bare
    ``host[:port]``. When a URL scheme is present it determines TLS; otherwise TLS is
    inferred from the port (``443`` -> secure). The returned host preserves the
    ``host[:port]`` form as provided, with any scheme stripped.
    """
    parsed = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
    if parsed.scheme:
        secure = parsed.scheme == "https"
    else:
        secure = parsed.port == 443
    return parsed.netloc, secure


def generate_minio_artifact_secret_manifest(
    template: Template, profile: str, access_key: str, secret_key: str
) -> KubernetesManifest:
    """Generate the ``mlpipeline-minio-artifact`` Secret manifest for a profile."""
    secret_info = K8sSecretManifestInfo(
        name=MLPIPELINE_MINIO_ARTIFACT_SECRET_NAME,
        namespace=None if profile == "*" else profile,
        data={
            MINIO_SECRET_ACCESS_KEY: base64.b64encode(access_key.encode()).decode("utf-8"),
            MINIO_SECRET_SECRET_KEY: base64.b64encode(secret_key.encode()).decode("utf-8"),
        },
        labels=None,
    )
    rendered = template.render(secret=secret_info)
    return KubernetesManifest(rendered)


def generate_artifact_repositories_configmap_manifest(
    template: Template, profile: str, bucket: str, endpoint: str
) -> KubernetesManifest:
    """Generate the argo ``artifact-repositories`` ConfigMap manifest for a profile."""
    host, secure = _parse_s3_endpoint(endpoint)
    rendered = template.render(
        name=ARTIFACT_REPOSITORIES_CONFIGMAP_NAME,
        namespace=None if profile == "*" else profile,
        annotation_key=ARTIFACT_REPOSITORY_ANNOTATION,
        annotation_ref=ARTIFACT_REPOSITORY_REF,
        secret_name=MLPIPELINE_MINIO_ARTIFACT_SECRET_NAME,
        access_key=MINIO_SECRET_ACCESS_KEY,
        secret_key=MINIO_SECRET_SECRET_KEY,
        bucket=bucket,
        endpoint=host,
        insecure=not secure,
        key_format=ARTIFACT_KEY_FORMAT,
    )
    return KubernetesManifest(rendered)


def generate_kfp_launcher_configmap_manifest(
    template: Template,
    profile: str,
    endpoint: str,
    region: str | None,
    default_pipeline_root: str,
) -> KubernetesManifest:
    """Generate the ``kfp-launcher`` ConfigMap manifest for a profile."""
    host, secure = _parse_s3_endpoint(endpoint)
    rendered = template.render(
        name=KFP_LAUNCHER_CONFIGMAP_NAME,
        namespace=None if profile == "*" else profile,
        default_pipeline_root=default_pipeline_root,
        endpoint=host,
        disable_ssl=not secure,
        region=region or "",
        secret_name=MLPIPELINE_MINIO_ARTIFACT_SECRET_NAME,
        access_key=MINIO_SECRET_ACCESS_KEY,
        secret_key=MINIO_SECRET_SECRET_KEY,
    )
    return KubernetesManifest(rendered)
