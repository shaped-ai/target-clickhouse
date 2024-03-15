
import datetime
import logging

from singer_sdk.testing.suites import TestSuite
from sqlalchemy import text

from tests.conftest import TargetClickhouseFileTestTemplate

logger = logging.getLogger(__name__)

class TestDateTypeTargetClickhouse(TargetClickhouseFileTestTemplate):
    """Test date type can be ingested into Clickhouse."""

    name = "date_type"

    def validate(self) -> None:
        """Validate the data in the target."""
        connector = self.target.default_sink_class.connector_class(self.target.config)
        result = connector.connection.execute(
            statement=text("SELECT * FROM date_type"),
        ).fetchall()
        record_id_1 = 1
        record_1 = [
            record for record in result if record[0] == record_id_1
        ][0]
        assert record_1[1] == datetime.date(2024, 3, 15)
        record_id_2 = 2
        record_2 = [
            record for record in result if record[0] == record_id_2
        ][0]
        assert record_2[1] == datetime.date(2024, 3, 16)

custom_target_test_suite = TestSuite(
    kind="target",
    tests=[
        TestDateTypeTargetClickhouse,
    ],
)
