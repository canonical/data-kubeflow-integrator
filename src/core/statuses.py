#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""File containing all possible statuses for Kubeflow Integrator charm."""

from enum import Enum

from data_platform_helpers.advanced_statuses.models import StatusObject


class CharmStatuses(Enum):
    """Generic status objects related to the charm."""

    ACTIVE_IDLE = StatusObject(status="active", message="")

    @staticmethod
    def missing_integration_with_opensearch() -> StatusObject:
        """Integration with Opensearch is required."""
        return StatusObject(
            status="blocked",
            message="Missing relation with: OpenSearch",
            action="Integrate with an OpenSearch Charm",
        )

    @staticmethod
    def missing_integration_with_mysql() -> StatusObject:
        """Integration with Mysql is required."""
        return StatusObject(
            status="blocked",
            message="Missing relation with: Mysql",
            action="Integrate with a Mysql Charm",
        )

    @staticmethod
    def missing_integration_with_postgresql() -> StatusObject:
        """Integration with Postgresql is required."""
        return StatusObject(
            status="blocked",
            message="Missing relation with: Postgresql",
            action="Integrate with a Postgresql Charm",
        )

    @staticmethod
    def missing_integration_with_mongodb() -> StatusObject:
        """Integration with Mongodb is required."""
        return StatusObject(
            status="blocked",
            message="Missing relation with: Mongodb",
            action="Integrate with a Mongodb Charm",
        )

    @staticmethod
    def missing_integration_with_kafka() -> StatusObject:
        """Integration with Kafka is required."""
        return StatusObject(
            status="blocked",
            message="Missing relation with: Kafka",
            action="Integrate with a Kafka Charm",
        )


class ConfigStatuses(Enum):
    """Status objects related to config options."""

    @staticmethod
    def missing_config_parameters(fields: list[str]) -> StatusObject:
        """Some of the mandatory config values are missing."""
        fields_str = ", ".join(f"'{field}'" for field in fields)
        return StatusObject(
            status="blocked",
            message=f"Missing config(s): {fields_str}",
            action=f"Set config(s): {fields_str}",
        )

    @staticmethod
    def invalid_config_parameters(fields: list[str]) -> StatusObject:
        """Some of the config values are invalid."""
        fields_str = ", ".join(f"'{field}'" for field in fields)
        return StatusObject(
            status="blocked",
            message=f"Invalid config(s): {fields_str}",
            action=f"Fix invalid config(s): {fields_str}",
        )
