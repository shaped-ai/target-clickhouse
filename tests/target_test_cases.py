import datetime
import logging
from typing import Any

from singer_sdk.testing.suites import TestSuite
from sqlalchemy import text

from target_clickhouse.connectors import ClickhouseConnector
from tests.conftest import TargetClickhouseFileTestTemplate

logger = logging.getLogger(__name__)


class TestConnector(ClickhouseConnector):
    """Test connector with additional methods for testing."""

    def get_table_ddl(self, table_name: str) -> str:  # noqa: S608
        """
        Get the DDL for a table.

        Args:
            table_name: The name of the table.

        Returns:
            The DDL for the table.

        """
        with self._connect() as conn:
            result = conn.execute(
                text(f"SHOW CREATE TABLE {table_name}"),
            ).fetchall()
            return result[0][0]

    def get_table_records(self, table_name: str) -> list[Any]:
        """
        Get all records from a table.

        Args:
            table_name: The name of the table.

        Returns:
            List of records.

        """
        with self._connect() as conn:
            # ruff: noqa: S608
            return conn.execute(text(f"SELECT * FROM {table_name}")).fetchall()


class TestDateTypeTargetClickhouse(TargetClickhouseFileTestTemplate):
    """Test date type, and null date type can be ingested into Clickhouse."""

    name = "date_type"

    def validate(self) -> None:
        """Validate the data in the target."""
        connector = TestConnector(self.target.config)
        records = {
            1: datetime.date(2024, 3, 15),
            2: datetime.date(2024, 3, 16),
            3: datetime.date(1920, 3, 16),
            4: None,
        }

        result = connector.get_table_records("date_type")

        for record_id, expected_date in records.items():
            record = next((rec for rec in result if rec[0] == record_id), None)
            assert record is not None, f"Record with id {record_id} not found"
            assert record[1] == (
                expected_date if expected_date is None else str(expected_date)
            ), f"For record {record_id}, expected {expected_date}, got {record[1]}"


class TestPrimaryKeyMaterialized(TargetClickhouseFileTestTemplate):
    """
    Test that primary keys are correctly materialized.

    This test verifies behavior when materialize_primary_keys=True.
    """

    name = "primary_key_materialized"

    def validate(self) -> None:
        """Validate the table structure in ClickHouse."""
        connector = TestConnector(self.target.config)

        # Check if primary key is materialized
        create_table_ddl = connector.get_table_ddl("primary_key_materialized")
        assert (
            "PRIMARY KEY id" in create_table_ddl
        ), "Expected PRIMARY KEY id in table DDL"


class TestPrimaryKeyNotMaterialized(TargetClickhouseFileTestTemplate):
    """
    Test that primary keys are not materialized.

    This test verifies behavior when materialize_primary_keys=False.
    """

    name = "primary_key_not_materialized"

    def validate(self) -> None:
        """Validate the table structure in ClickHouse."""
        connector = TestConnector(self.target.config)

        # Check if primary key is not materialized
        create_table_ddl = connector.get_table_ddl("primary_key_not_materialized")
        assert (
            "PRIMARY KEY" not in create_table_ddl
        ), "Expected no PRIMARY KEY in table DDL"
        assert "ORDER BY id" in create_table_ddl, "Expected ORDER BY id in table DDL"


# Combined test suite with all test cases
custom_target_test_suite = TestSuite(
    kind="target",
    tests=[TestDateTypeTargetClickhouse, TestPrimaryKeyMaterialized],
)

# Combined test suite with non-materialized primary keys config.
custom_target_test_suite_non_materialized_primary_keys = TestSuite(
    kind="target",
    tests=[TestPrimaryKeyNotMaterialized],
)
