#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import base64
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


def test_charm_block_when_integrated_with_mysql(base_state: State, charm_configuration: dict):
    """Check that charm will be in a blocked status when integrated with Mysql."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    state_in = base_state
    # When:
    state_out = ctx.run(ctx.on.start(), state_in)
    # Then:
    assert state_out.unit_status == ActiveStatus()

    # Given
    mysql_relation = Relation(endpoint="mysql")
    relations = state_out.relations.union([mysql_relation])
    state_in = dataclasses.replace(state_out, relations=relations)

    # When
    state_out = ctx.run(ctx.on.relation_changed(mysql_relation), state_in)

    # Then:
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing config(s): 'mysql-database-name'" in status.message


def test_charm_generate_secret_manifests_when_integrated(
    base_state: State, charm_configuration: dict
):
    """Test that charm will generate manifests when integrated with mysql and the secrets manifests provider."""
    charm_configuration["options"]["profile"]["default"] = "profile-name"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    state_in = base_state
    # When:
    state_out = ctx.run(ctx.on.start(), state_in)
    # Then:
    assert state_out.app_status == ActiveStatus()

    # Given
    charm_configuration["options"]["mysql-database-name"]["default"] = "database"
    ctx.config = charm_configuration
    state_in = state_out
    # When:
    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # Then:
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: Mysql" in status.message

    # Given:
    mysql_relation = Relation(
        endpoint="mysql",
        remote_app_data={
            "username": "user-test",
            "password": "user-password",
            "database": "database",
            "tls-ca": "",
        },
    )
    relations = state_out.relations.union([mysql_relation])
    state_in = dataclasses.replace(state_out, relations=relations)

    # When:
    state_out = ctx.run(ctx.on.relation_changed(mysql_relation), state_in)
    # Then:
    assert state_out.app_status == ActiveStatus()

    # Given:
    secrets_manifests_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    relations = state_out.relations.union([secrets_manifests_relation])
    state_in = dataclasses.replace(state_out, relations=relations)

    # When:
    state_out = ctx.run(ctx.on.relation_changed(secrets_manifests_relation), state_in)

    # Then:
    assert state_out.app_status == ActiveStatus()
    # Make sure the manifest is generated
    kubernetes_manifests = state_out.get_relation(secrets_manifests_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    generated_secret = kubernetes_manifests[0]
    assert generated_secret["apiVersion"] == "v1"
    assert generated_secret["kind"] == "Secret"

    assert generated_secret["data"]["MYSQL_USERNAME"] == base64.b64encode(b"user-test").decode()
    assert (
        generated_secret["data"]["MYSQL_PASSWORD"] == base64.b64encode(b"user-password").decode()
    )


def test_charm_generate_no_namespace_secret_manifests_when_integrated(
    base_state: State, charm_configuration: dict
):
    """Test that charm will generate secret manifests with no namespace when profile config param is set to "*"."""
    charm_configuration["options"]["profile"]["default"] = "*"
    ctx = testing.Context(
        KubeflowIntegratorCharm,
        meta=METADATA,
        config=charm_configuration,
        actions=ACTIONS,
        unit_id=0,
    )

    state_in = base_state
    # When:
    state_out = ctx.run(ctx.on.start(), state_in)
    # Then:
    assert state_out.app_status == ActiveStatus()

    # Given
    charm_configuration["options"]["mysql-database-name"]["default"] = "database"
    ctx.config = charm_configuration
    state_in = state_out
    # When:
    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # Then:
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: Mysql" in status.message

    # Given:
    mysql_relation = Relation(
        endpoint="mysql",
        remote_app_data={
            "username": "user-test",
            "password": "user-password",
            "database": "database",
            "tls-ca": "",
        },
    )
    relations = state_out.relations.union([mysql_relation])
    state_in = dataclasses.replace(state_out, relations=relations)

    # When:
    state_out = ctx.run(ctx.on.relation_changed(mysql_relation), state_in)
    # Then:
    assert state_out.app_status == ActiveStatus()

    # Given:
    secrets_manifests_relation = Relation(endpoint="secrets", interface="kubernetes_manifest")
    relations = state_out.relations.union([secrets_manifests_relation])
    state_in = dataclasses.replace(state_out, relations=relations)

    # When:
    state_out = ctx.run(ctx.on.relation_changed(secrets_manifests_relation), state_in)

    # Then:
    assert state_out.app_status == ActiveStatus()
    # Make sure the manifest is generated
    kubernetes_manifests = state_out.get_relation(secrets_manifests_relation.id).local_app_data[
        "kubernetes_manifests"
    ]
    kubernetes_manifests = json.loads(kubernetes_manifests)
    generated_secret = kubernetes_manifests[0]
    assert generated_secret["apiVersion"] == "v1"
    assert generated_secret["kind"] == "Secret"

    assert generated_secret["data"]["MYSQL_USERNAME"] == base64.b64encode(b"user-test").decode()
    assert (
        generated_secret["data"]["MYSQL_PASSWORD"] == base64.b64encode(b"user-password").decode()
    )
    assert "namespace" not in generated_secret["metadata"]
