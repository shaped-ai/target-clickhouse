from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_clickhouse.sinks import (
    ClickhouseSink,
)


class TargetClickhouse(SQLTarget):
    """SQL-based target for Clickhouse."""

    name = "target-clickhouse"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="The SQLAlchemy connection string for the ClickHouse database",
        ),
        th.Property(
            "table_name",
            th.StringType,
            description="The name of the table to write to. Defaults to stream name.",
        ),
    ).to_dict()

    default_sink_class = ClickhouseSink

if __name__ == "__main__":
    TargetClickhouse.cli()
