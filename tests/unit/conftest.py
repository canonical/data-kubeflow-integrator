#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
from pathlib import Path

import yaml
from ops.testing import PeerRelation, State
from pytest import fixture




CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())



@fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


@fixture
def base_state() -> State:
    status_peers = PeerRelation(endpoint="status-peers")
    return State(
        leader=True,
        relations=[
            status_peers,
        ],
    )
