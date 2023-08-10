"""clickhouse target sink class, which handles writing streams."""

from __future__ import annotations

from typing import Any, Iterable

import simplejson as json
from singer_sdk.sinks import SQLSink

from target_clickhouse.connectors import ClickhouseConnector


class ClickhouseSink(SQLSink):
    """clickhouse target sink class."""

    connector_class = ClickhouseConnector

    @property
    def full_table_name(self) -> str:
        """Return the fully qualified table name.

        Returns
            The fully qualified table name.
        """
        # Use the config table name if set.
        if self.config.get("table_name"):
            return self.config.get("table_name")

        return self.connector.get_fully_qualified_name(
            table_name=self.table_name,
            schema_name=self.schema_name,
            db_name=self.database_name,
        )

    def bulk_insert_records(
            self,
            full_table_name: str,
            schema: dict,
            records: Iterable[dict[str, Any]],
        ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        # Need to convert any records with a dict type to a JSON string.
        for record in records:
            for key, value in record.items():
                if isinstance(value, dict):
                    record[key] = json.dumps(value)

        return super().bulk_insert_records(full_table_name, schema, records)
