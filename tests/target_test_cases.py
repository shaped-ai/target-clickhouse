
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
        result = connector.connection.execute(
            statement=text("SELECT * FROM date_type"),
        ).fetchall()
        record_id_1 = 1
        record_1 = next(iter([
            record for record in result if record[0] == record_id_1
        ]))
        assert record_1[1] == datetime.date(2024, 3, 15)
        record_id_2 = 2
        record_2 = next(iter([
            record for record in result if record[0] == record_id_2
        ]))
        assert record_2[1] == datetime.date(2024, 3, 16)
        record_id_3 = 3
        record_3 = next(iter([
            record for record in result if record[0] == record_id_3
        ]))
        assert record_3[1] is None

custom_target_test_suite = TestSuite(
    kind="target",
    tests=[
        TestDateTypeTargetClickhouse,
    ],
)
