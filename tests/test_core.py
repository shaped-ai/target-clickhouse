"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import typing as t

import pytest
from singer_sdk.testing import get_target_test_class

from target_clickhouse.target import TargetClickhouse

SAMPLE_HTTP_CONFIG: dict[str, t.Any] = {
    "sqlalchemy_url": "clickhouse+http://default:@localhost:18123",
}

SAMPLE_NATIVE_CONFIG: dict[str, t.Any] = {
    "sqlalchemy_url": "clickhouse+native://default:@localhost:18123",
}

# Run standard built-in target tests from the SDK:
StandardHTTPTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=SAMPLE_HTTP_CONFIG,
)

StandardNativeTargetTests = get_target_test_class(
    target_class=TargetClickhouse,
    config=SAMPLE_HTTP_CONFIG,
)

class TestTargetClickhouseHttp(StandardHTTPTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Standard HTTP Connection Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        return "resource"

class TestTargetClickhouseNative(StandardNativeTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Standard Native Connection Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        return "resource"
