#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
import pytest

import jubilant

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
