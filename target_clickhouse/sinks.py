"""clickhouse target sink class, which handles writing streams."""

from __future__ import annotations

import decimal
from collections.abc import MutableMapping
from typing import Any, Iterable

import jsonschema.exceptions as jsonschema_exceptions
import simplejson as json
import sqlalchemy
from pendulum import now
from singer_sdk.sinks import SQLSink
from sqlalchemy.sql.expression import bindparam

from target_clickhouse.connectors import ClickhouseConnector


class ClickhouseSink(SQLSink):
    """clickhouse target sink class."""

    connector_class = ClickhouseConnector

    # Investigate larger batch sizes without OOM.
    MAX_SIZE_DEFAULT = 10000

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns
            Max number of records to batch before `is_full=True`
        """
        return self.MAX_SIZE_DEFAULT

    @property
    def full_table_name(self) -> str:
        """Return the fully qualified table name.

        Returns
            The fully qualified table name.
        """
        # Use the config table name if set.
        _table_name = self.config.get("table_name")

        if _table_name is not None:
            return _table_name

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

    def activate_version(self, new_version: int) -> None:
        """Bump the active version of the target table.

        Args:
            new_version: The version number to activate.
        """
        # There's nothing to do if the table doesn't exist yet
        # (which it won't the first time the stream is processed)
        if not self.connector.table_exists(self.full_table_name):
            return

        deleted_at = now()

        if not self.connector.column_exists(
            full_table_name=self.full_table_name,
            column_name=self.version_column_name,
        ):
            self.connector.prepare_column(
                self.full_table_name,
                self.version_column_name,
                sql_type=sqlalchemy.types.Integer(),
            )

        if self.config.get("hard_delete", True):
            with self.connector._connect() as conn, conn.begin(): # noqa: SLF001
                conn.execute(
                    sqlalchemy.text(
                        f"ALTER TABLE {self.full_table_name} DELETE "
                        f"WHERE {self.version_column_name} <= {new_version}",
                    ),
                )
            return

        if not self.connector.column_exists(
            full_table_name=self.full_table_name,
            column_name=self.soft_delete_column_name,
        ):
            self.connector.prepare_column(
                self.full_table_name,
                self.soft_delete_column_name,
                sql_type=sqlalchemy.types.DateTime(),
            )

        query = sqlalchemy.text(
            f"ALTER TABLE {self.full_table_name} \n"
            f"UPDATE {self.soft_delete_column_name} = :deletedate \n"
            f"WHERE {self.version_column_name} < :version \n"
            f"  AND {self.soft_delete_column_name} IS NULL\n",
        )
        query = query.bindparams(
            bindparam("deletedate", value=deleted_at, type_=sqlalchemy.types.DateTime),
            bindparam("version", value=new_version, type_=sqlalchemy.types.Integer),
        )
        with self.connector._connect() as conn, conn.begin(): # noqa: SLF001
            conn.execute(query)

    def _validate_and_parse(self, record: dict) -> dict:
        """Pre-validate and repair records for string type mismatches, then validate.

        Args:
            record: Individual record in the stream.

        Returns:
            Validated record.
        """
        # Pre-validate and correct string type mismatches.
        record = self._pre_validate_for_string_type(record)

        try:
            self._validator.validate(record)
            self._parse_timestamps_in_record(
                record=record,
                schema=self.schema,
                treatment=self.datetime_error_treatment,
            )
        except jsonschema_exceptions.ValidationError as e:
            if self.logger:
                self.logger.exception(f"Record failed validation: {record}")
            raise e # noqa: RERAISES

        return record

    def _pre_validate_for_string_type(self, record: dict) -> dict:
        """Pre-validate record for string type mismatches and correct them.

        Args:
            record: Individual record in the stream.

        Returns:
            Record with corrected string type mismatches.
        """
        for key, value in record.items():
            # Checking if the schema expects a string for this key.
            expected_type = self.schema.get("properties", {}).get(key, {}).get("type")
            if expected_type == "string" and not isinstance(value, str):
                # Convert the value to string if it's not already a string.
                record[key] = (
                    json.dumps(record[key])
                    if isinstance(value, (dict, list)) else str(value)
                )
                if self.logger:
                    self.logger.debug(
                        f"Converted field {key} to string: {record[key]}",
                    )

        return self._convert_decimal_to_float(record)

    def _convert_decimal_to_float(self, obj):
        """Recursively convert all Decimal values in a dictionary to floats.

        Args:
        obj: The input object (dictionary, list, or any other data type).

        Returns:
        The object with all Decimal values converted to strings.
        """
        if isinstance(obj, MutableMapping):
            for key, value in obj.items():
                obj[key] = self._convert_decimal_to_float(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                obj[i] = self._convert_decimal_to_float(item)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)

        return obj
