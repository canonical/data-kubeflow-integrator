#!/usr/bin/env python3

# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Definition of charm config model class."""

from __future__ import annotations

import re
from typing import Annotated, Any

from charms.data_platform_libs.v0.data_interfaces import KafkaRequires
from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import AfterValidator, BeforeValidator, Field, model_validator

PROFILE_REGEX = re.compile(r"^(\*|[a-z0-9]([-a-z0-9]*[a-z0-9])?)$")


def nullify_empty_string(in_str: str) -> str | None:
    """Replace empty str with None."""
    if not in_str:
        return None
    return in_str


def validate_topic_name(topic: str | None) -> str | None:
    """Validate topic name for Kafka."""
    if not topic:
        return None
    if KafkaRequires.is_topic_value_acceptable(topic):
        return topic
    else:
        raise ValueError(
            f"Trying to pass an invalid topic value: {topic}, please pass an acceptable value instead"
        )


class ProfileConfig(BaseConfigModel):
    """Model for the 'profile' configuration."""

    # Validate profile with k8s namespace regex following rfc1123
    profile: str = Field(pattern=PROFILE_REGEX)

    @model_validator(mode="before")
    @classmethod
    def remove_value_if_none(cls, data: Any) -> Any:
        """Remove value of profile if the value is None to show missing config."""
        if isinstance(data, dict):
            if "profile" in data:
                if data["profile"] is None:
                    data.pop("profile")
        return data


class OpenSearchConfig(BaseConfigModel):
    """Model for the opensearch configuration."""

    index_name: Annotated[str, BeforeValidator(nullify_empty_string)] = Field(
        alias="opensearch-index-name"
    )

    extra_user_roles: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        None, alias="opensearch-extra-user-roles"
    )

    @model_validator(mode="before")
    @classmethod
    def remove_value_if_none(cls, data: Any) -> Any:
        """Remove value of profile if the value is None to show missing config."""
        if isinstance(data, dict):
            if "opensearch-index-name" in data:
                if data["opensearch-index-name"] is None:
                    data.pop("opensearch-index-name")
        return data


class KafkaConfig(BaseConfigModel):
    """Model for the Kafka configuration."""

    kafka_topic_name: Annotated[
        str | None, BeforeValidator(nullify_empty_string), AfterValidator(validate_topic_name)
    ] = Field(alias="kafka-topic-name")

    kafka_extra_user_roles: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        None, alias="kafka-extra-user-roles"
    )

    kafka_consumer_group_prefix: Annotated[str | None, BeforeValidator(nullify_empty_string)] = (
        Field(None, alias="kafka-consumer-group-prefix")
    )


class MongoDbConfig(BaseConfigModel):
    """Model for MongoDb configuration."""

    mongodb_database_name: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        alias="mongodb-database-name"
    )

    mongodb_extra_user_roles: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        None, alias="mongodb-extra-user-roles"
    )


class MysqlConfig(BaseConfigModel):
    """Model for Mysql configuration."""

    mysql_database_name: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        alias="mysql-database-name"
    )

    mysql_extra_user_roles: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        None, alias="mysql-extra-user-roles"
    )


class PostgresqlConfig(BaseConfigModel):
    """Model for Postgresql configuration."""

    postrgesql_database_name: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        alias="postgresql-database-name"
    )

    postgresql_extra_user_roles: Annotated[str | None, BeforeValidator(nullify_empty_string)] = (
        Field(None, alias="postgresql-extra-user-roles")
    )


class SparkConfig(BaseConfigModel):
    """Model for Spark configuration."""

    spark_service_account: Annotated[str | None, BeforeValidator(nullify_empty_string)] = Field(
        alias="spark-service-account"
    )
