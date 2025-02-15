"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

from singer_sdk.testing import get_target_test_class

from target_clickhouse.target import TargetClickhouse
from tests.target_test_cases import (
    custom_target_test_suite,
    custom_target_test_suite_non_materialized_primary_keys,
)

TEST_CONFIG: dict[str, t.Any] = {
    "sqlalchemy_url": "clickhouse+http://default:@localhost:18123",
}

TEST_CONFIG_SPREAD: dict[str, t.Any] = {
    "driver": "http",
    "host": "localhost",
    "port": 18123,
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
    "optimize_after": True,
}

TEST_CONFIG_NATIVE: dict[str, t.Any] = {
    "driver": "native",
    "host": "localhost",
    "port": 19000,
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
}

TEST_CONFIG_NON_MATERIALIZED_PRIMARY_KEYS: dict[str, t.Any] = {
    "driver": "http",
    "host": "localhost",
    "port": 18123,
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
    "materialize_primary_keys": False,
}

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=TEST_CONFIG,
    custom_suites=[custom_target_test_suite],
)


class TestStandardTargetClickhouse(
    StandardTargetTests,  # type: ignore[valid-type]
):
    """Standard Target Tests."""


SpreadTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=TEST_CONFIG_SPREAD,
    custom_suites=[custom_target_test_suite],
)


class TestSpreadTargetClickhouse(SpreadTargetTests):  # type: ignore[valid-type]
    """Standard Target Tests."""


NonMaterializedPrimaryKeysTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=TEST_CONFIG_NON_MATERIALIZED_PRIMARY_KEYS,
    custom_suites=[custom_target_test_suite_non_materialized_primary_keys],
)


class TestNonMaterializedPrimaryKeysTargetClickhouse(
    NonMaterializedPrimaryKeysTargetTests  # type: ignore[valid-type]
):
    """Standard Target Tests."""
