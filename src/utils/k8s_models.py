#!/usr/bin/env python3
# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.

"""A set of structured models used to generate kubernetes manifests."""

from dataclasses import dataclass, field

from charms.resource_dispatcher.v0.kubernetes_manifests import (
    KubernetesManifest,  # type: ignore[import-untyped]
)
from pydantic import BaseModel, Field, model_validator


class K8sSecretManifestInfo(BaseModel):
    """Structured model used to group info regarding a secret, to be used to generate k8s secret manifest."""

    name: str
    namespace: str | None
    labels: dict[str, str] | None = Field(None)
    data: dict[str, str]


class EnvVarFromSecret(BaseModel):
    """Structured model for environment variable from secret in poddefault."""

    secret_name: str
    secret_key: str
    optional: bool = False


class PodDefaultEnvVar(BaseModel):
    """Structured model for environment variable definitions in poddefault."""

    name: str
    value: str | None = None
    secret: EnvVarFromSecret | None = None

    @model_validator(mode="after")
    def check_on_of(self):
        """Validate that either the `value` field or `secret` field is specified."""
        if not (self.value or self.secret):
            raise ValueError("Either 'value' or 'secret' must be provided")
        return self


class PodDefaultSecretVolume(BaseModel):
    """Structured model for volumes mounted on pods from secrets."""

    name: str
    secret_name: str
    mount_path: str


class PodDefaultAnnotation(BaseModel):
    """Structured model for annotations in a pod-default."""

    key: str
    value: str


class K8sPodDefaultManifestInfo(BaseModel):
    """Structured model used to group info regarding a pod default, to be used to generate k8s poddefault manifest."""

    name: str
    namespace: str | None
    desc: str
    selector_name: str
    env_vars: list[PodDefaultEnvVar]
    secret_volumes: list[PodDefaultSecretVolume] | None = None
    annotations: list[PodDefaultAnnotation] | None = None
    args: list[str] | None = None


@dataclass
class ReconciledManifests:
    """Structured model used to group the generated secrets and send them to the provider."""

    secrets: list[KubernetesManifest] = field(default_factory=list)
    poddefaults: list[KubernetesManifest] = field(default_factory=list)
    serviceaccounts: list[KubernetesManifest] = field(default_factory=list)
    roles: list[KubernetesManifest] = field(default_factory=list)
    role_bindings: list[KubernetesManifest] = field(default_factory=list)

    def __add__(self, other: "ReconciledManifests") -> "ReconciledManifests":
        """Implements the add interface."""
        if not isinstance(other, ReconciledManifests):
            return NotImplemented

        return ReconciledManifests(
            secrets=self.secrets + other.secrets,
            poddefaults=self.poddefaults + other.poddefaults,
            serviceaccounts=self.serviceaccounts + other.serviceaccounts,
            roles=self.roles + other.roles,
            role_bindings=self.role_bindings + other.role_bindings,
        )

    def __iadd__(self, other: "ReconciledManifests") -> "ReconciledManifests":
        """Implement the add in-place."""
        if not isinstance(other, ReconciledManifests):
            return NotImplemented

        self.secrets += other.secrets
        self.poddefaults += other.poddefaults
        self.serviceaccounts += other.serviceaccounts
        self.roles += other.roles
        self.role_bindings += other.role_bindings
        return self
