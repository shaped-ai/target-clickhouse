"""Test Configuration."""

from pathlib import Path

import docker
import pytest
from singer_sdk.testing.templates import TargetFileTestTemplate

pytest_plugins = ()


@pytest.fixture(scope="session")
def clickhouse_container():
    """
    Create a ClickHouse container for testing.

    In CI, this fixture is a no-op since ClickHouse is provided via GitHub Actions
    services.
    """
    import os
    import time

    import requests

    # Skip container creation if we're in GitHub Actions
    if os.getenv("GITHUB_ACTIONS"):
        # Just wait for the service to be ready
        while (
            requests.get("http://localhost:18123/ping", timeout=1).text.strip() != "Ok."
        ):
            time.sleep(1)
        yield None
        return

    # Local development setup
    client = docker.from_env()
    client.images.pull("clickhouse/clickhouse-server", tag="24.10-alpine")

    container = client.containers.run(
        "clickhouse/clickhouse-server:24.10-alpine",
        name="clickhouse-server",
        ports={"8123/tcp": 18123, "9000/tcp": 19000},
        ulimits=[docker.types.Ulimit(name="nofile", soft=262144, hard=262144)],
        environment={
            "CLICKHOUSE_DB": "default",
            "CLICKHOUSE_USER": "default",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
        },
        detach=True,
        remove=True,
    )

    # Wait for ClickHouse to be ready
    while requests.get("http://localhost:18123/ping", timeout=1).text.strip() != "Ok.":
        time.sleep(1)

    yield container
    container.stop()


class TargetClickhouseFileTestTemplate(TargetFileTestTemplate):
    """
    Base Target File Test Template.

    Use this when sourcing Target test input from a .singer file.
    """

    @pytest.fixture(autouse=True)
    def _setup_clickhouse(self, clickhouse_container):
        """Set up ClickHouse container for tests."""
        self.clickhouse_container = clickhouse_container

    @property
    def singer_filepath(self):
        """
        Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns
            The expected Path to this tests singer file.

        """
        current_file_path = Path(__file__).resolve()
        return current_file_path.parent / "target_test_streams" / f"{self.name}.singer"
