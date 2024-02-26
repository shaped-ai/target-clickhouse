"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

from importlib.abc import Traversable
import os
import typing as t
from pathlib import Path

from singer_sdk.testing import get_target_test_class, suites
from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.templates import TargetFileTestTemplate

from target_clickhouse.target import TargetClickhouse

TEST_CONFIG: dict[str, t.Any] = {
    "sqlalchemy_url": "clickhouse+http://default:@localhost:8123"
}

TEST_CONFIG_SPREAD: dict[str, t.Any] = {
    "driver": "http",
    "host": "localhost",
    "port": 8123,
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
    "port": 9000,
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
}

class TargetAllTypesTest(TargetFileTestTemplate):
    """Test Target handles array data."""
    name = "all_types"

    @property
    def singer_filepath(self) -> Traversable:
        """Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns:
            The expected Path to this tests singer file.
        """
        return Path(os.path.abspath(os.path.join(os.path.join(os.path.join(__file__, os.pardir), "resources"), f"{self.name}.singer")))

custom_test_key_properties = suites.TestSuite(
    kind="target",
    tests=[TargetAllTypesTest]
)
# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=TEST_CONFIG,
    custom_suites=[custom_test_key_properties],
)


class TestStandardTargetClickhouse(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""


SpreadTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=TEST_CONFIG_SPREAD,
)

class TestSpreadTargetClickhouse(SpreadTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

