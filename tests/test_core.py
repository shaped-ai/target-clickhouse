"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from clickhouse_driver import Client
from singer_sdk.testing import get_target_test_class

from target_clickhouse.target import TargetClickhouse

TEST_CONFIG: dict[str, t.Any] = {
    "sqlalchemy_url": "clickhouse+http://default:@localhost:18123",
}

# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=TEST_CONFIG,
)

class TestTargetClickhouse(StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""

    @pytest.fixture(autouse=True)
    def _resource(self) -> None:
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        _ = Client(
            host="localhost",
            port=19000,
            user="default",
            password="",
            database="default",
        )
