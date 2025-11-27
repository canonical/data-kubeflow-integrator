#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

import pytest
import yaml
from ops import ActiveStatus, BlockedStatus, testing
from ops.testing import State

from src.charm import KubeflowIntegratorCharm

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.mark.parametrize(
    "invalid_profile",
    ["MyProfile", "profile!", "-profile", "frontend-", "my_profile", " profile ", ""],
)
def test_invalid_profile(charm_configuration: dict, base_state: State, invalid_profile):
    """Test that charm will be blocked with an invalid profile."""
    # Given
    charm_configuration["options"]["profile"]["default"] = invalid_profile

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

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Invalid config(s): 'profile'" in status.message


def test_charm_blocked_on_opensearch_index_specified(charm_configuration: dict, base_state: State):
    """Test that charm will be in a blocked state waiting for an integration with opensearch if 'opensearch-index-name' is specified."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
    charm_configuration["options"]["opensearch-index-name"]["default"] = "index"

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

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: OpenSearch" in status.message


def test_charm_blocked_on_postgresql_database_name_specified(
    charm_configuration: dict, base_state: State
):
    """Test that charm will be in a blocked state waiting for an integration with postgresql if 'postgresql-database-name' is specified."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
    charm_configuration["options"]["postgresql-database-name"]["default"] = "mydb"

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

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: Postgresql" in status.message


def test_charm_blocked_on_mongodb_database_name_specified(
    charm_configuration: dict, base_state: State
):
    """Test that charm will be in a blocked state waiting for an integration with mongodb if 'mongodb-database-name' is specified."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
    charm_configuration["options"]["mongodb-database-name"]["default"] = "mydb"

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

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: MongoDB" in status.message


def test_charm_blocked_on_mysql_database_specified(charm_configuration: dict, base_state: State):
    """Test that charm will be in a blocked state waiting for an integration with mongodb if 'mysql-database-name' is specified."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
    charm_configuration["options"]["mysql-database-name"]["default"] = "mydb"

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

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: Mysql" in status.message


def test_charm_blocked_on_kafka_topic_specified(charm_configuration: dict, base_state: State):
    """Test that charm will be in a blocked state waiting for an integration with kafka if 'kafka-topic-name' is specified."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
    charm_configuration["options"]["kafka-topic-name"]["default"] = "index"

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

    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: Kafka" in status.message


def test_charm_blocked_on_spark_serviceaccount_specified_without_spark_relation(
    charm_configuration: dict, base_state: State
):
    """Test that charm will be in a blocked state waiting for an integration with Spark if 'spark-service-account' is specified."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
    charm_configuration["options"]["spark-service-account"]["default"] = "spark"

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
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing relation with: Spark" in status.message


def test_config_changed(charm_configuration: dict, base_state: State):
    """Test that charm will be in a blocked state once the profile config becomes invalid or missing."""
    # Given
    charm_configuration["options"]["profile"]["default"] = "profile"
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

    # When
    charm_configuration["options"]["profile"]["default"] = " profile "
    ctx.config = charm_configuration

    state_out = ctx.run(ctx.on.config_changed(), state_out)

    # Then:
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Invalid config(s): 'profile'" in status.message

    # When:
    charm_configuration["options"]["profile"]["default"] = None
    ctx.config = charm_configuration
    state_out = ctx.run(ctx.on.config_changed(), state_out)

    # Then:
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing config(s): 'profile'" in status.message

    # When:
    charm_configuration["options"]["profile"]["default"] = "profile"

    ctx.config = charm_configuration
    state_out = ctx.run(ctx.on.config_changed(), state_out)

    # Then:
    assert state_out.app_status == ActiveStatus()
