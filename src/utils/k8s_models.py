from dataclasses import dataclass, field
from charms.resource_dispatcher.v0.kubernetes_manifests import KubernetesManifest
from pydantic import BaseModel, Field, model_validator


class K8sSecretManifestInfo(BaseModel):
    """Structured model used to group info regarding a secret, to be used to generate k8s secret manifest."""

    name: str
    namespace: str
    labels: dict[str, str] | None = Field(None)
    data: dict[str, str]


class EnvVarFromSecret(BaseModel):
    """Structured model for environment variable from secret in poddefault"""

    secret_name: str
    secret_key: str
    optional: bool = False


class PodDefaultEnvVar(BaseModel):
    """Structured model for environment variable definitions in poddefault"""

    name: str
    value: str | None = None
    secret: EnvVarFromSecret | None = None

    @model_validator(mode="after")
    def check_on_of(self):
        if not (self.value or self.secret):
            raise ValueError("Either 'value' or 'secret' must be provided")
        return self


class K8sPodDefaultManifestInfo(BaseModel):
    """Structured model used to group info regarding a pod defautl, to be used to generate k8s poddefault manifest."""

    name: str
    namespace: str
    desc: str
    selector_name: str
    env_vars: list[PodDefaultEnvVar]


@dataclass
class ReconciledManifests:
    secrets: list[KubernetesManifest] = field(default_factory=list)
    poddefaults: list[KubernetesManifest] = field(default_factory=list)
    serviceaccounts: list[KubernetesManifest] = field(default_factory=list)

    def __add__(self, other: "ReconciledManifests") -> "ReconciledManifests":
        if not isinstance(other, ReconciledManifests):
            return NotImplemented

        return ReconciledManifests(
            secrets=self.secrets + other.secrets,
            poddefaults=self.poddefaults + other.poddefaults,
            serviceaccounts=self.serviceaccounts + other.serviceaccounts,
        )

    def __iadd__(self, other: "ReconciledManifests") -> "ReconciledManifests":
        if not isinstance(other, ReconciledManifests):
            return NotImplemented

        self.secrets += other.secrets
        self.poddefaults += other.poddefaults
        self.serviceaccounts += other.serviceaccounts
        return self
