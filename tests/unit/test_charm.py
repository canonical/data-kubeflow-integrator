# Copyright 2025 Ubuntu
# See LICENSE file for licensing details.
#
# To learn more about testing, see https://ops.readthedocs.io/en/latest/explanation/testing.html

from pathlib import Path

import yaml
from ops import ActiveStatus, BlockedStatus, testing
from ops.testing import State

from charm import KubeflowIntegratorCharm

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


def test_charm_blocked_on_start(base_state: State):
    """Test that the charm has the correct state after handling the start event."""
    # Given
    ctx = testing.Context(KubeflowIntegratorCharm)
    state_in = base_state
    # When:
    state_out = ctx.run(ctx.on.start(), state_in)
    # Then:
    assert isinstance(status := state_out.app_status, BlockedStatus)
    assert "Missing config(s): 'profile'" in status.message


def test_charm_start_ok(base_state: State, charm_configuration: dict):
    """Test that charm starts ok if the 'profile' config parameter is provided."""
    # Given
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
