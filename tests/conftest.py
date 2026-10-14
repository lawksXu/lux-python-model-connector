"""Opt-in isolated MySQL integration fixtures; never use application databases."""

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pymysql
import pytest


@dataclass(frozen=True)
class TestServices:
    host: str
    port: int
    user: str
    password: str
    kafka: str | None = None


@pytest.fixture
def mysql_settings() -> Iterator:
    from lux_model_connector.store import MySQLSettings

    config = os.environ.get("LUX_CONNECTOR_TEST_CONFIG")
    if not config:
        pytest.skip("Set LUX_CONNECTOR_TEST_CONFIG for isolated MySQL integration")
    services = TestServices(**json.loads(Path(config).read_text()))
    database = "lux_connector_test_" + uuid4().hex
    connection = pymysql.connect(
        host=services.host,
        port=services.port,
        user=services.user,
        password=services.password,
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4")
        yield MySQLSettings(
            host=services.host,
            port=services.port,
            user=services.user,
            password=services.password,
            database=database,
        )
    finally:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE `{database}`")
        connection.close()


@pytest.fixture
def store(mysql_settings):
    from lux_model_connector.store import ConnectorStore

    result = ConnectorStore(mysql_settings, result_base_url="http://connector.test")
    result.initialize()
    return result
