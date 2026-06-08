import datetime
import logging

from singer_sdk.testing.suites import TestSuite
from sqlalchemy import text

from tests.conftest import TargetClickhouseFileTestTemplate

logger = logging.getLogger(__name__)


class TestDateTypeTargetClickhouse(TargetClickhouseFileTestTemplate):
    """Test date type, and null date type can be ingested into Clickhouse."""

    name = "date_type"

    def validate(self) -> None:
        """Validate the data in the target."""
        connector = self.target.default_sink_class.connector_class(self.target.config)
        records = {
            1: datetime.date(2024, 3, 15),
            2: datetime.date(2024, 3, 16),
            3: datetime.date(1920, 3, 16),
            4: None,
        }

        result = connector.connection.execute(
            statement=text("SELECT * FROM date_type"),
        ).fetchall()

        for record_id, expected_date in records.items():
            record = next((rec for rec in result if rec[0] == record_id), None)
            assert record is not None, f"Record with id {record_id} not found"
            assert record[1] == (
                expected_date if expected_date is None else str(expected_date)
            ), f"For record {record_id}, expected {expected_date}, got {record[1]}"


custom_target_test_suite = TestSuite(
    kind="target",
    tests=[
        TestDateTypeTargetClickhouse,
    ],
)
