from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_clickhouse.engine_class import SupportedEngines
from target_clickhouse.sinks import (
    ClickhouseSink,
)


class TargetClickhouse(SQLTarget):
    """SQL-based target for Clickhouse."""

    name = "target-clickhouse"

    config_jsonschema = th.PropertiesList(
        # connection properties
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="The SQLAlchemy connection string for the ClickHouse database. "
                        "Used if set, otherwise separate settings are used",
        ),
        th.Property(
            "driver",
            th.StringType,
            required=False,
            description="Driver type",
            default="http",
            allowed_values=["http", "native", "asynch"],
        ),
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Database user",
            default="default",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="Username password",
            secret=True,
        ),
        th.Property(
            "host",
            th.StringType,
            required=False,
            description="Database host",
            default="localhost",
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=False,
            description="Database connection port",
            default=8123,
        ),
        th.Property(
            "database",
            th.StringType,
            required=False,
            description="Database name",
            default="default",
        ),
        th.Property(
            "secure",
            th.BooleanType,
            required=False,
            description="Should the connection be secure",
            default=False,
        ),
        th.Property(
            "verify",
            th.BooleanType,
            description="Should secure connection need to verify SSL/TLS",
            default=True,
        ),

        # other settings
        th.Property(
            "engine_type",
            th.StringType,
            required=False,
            description="The engine type to use for the table.",
            allowed_values=[e.value for e in SupportedEngines],
        ),
        th.Property(
            "table_name",
            th.StringType,
            required=False,
            description="The name of the table to write to. Defaults to stream name.",
        ),
        th.Property(
            "table_path",
            th.StringType,
            required=False,
            description="The table path for replicated tables. This is required when "
                        "using any of the replication engines. Check out the "
                        "[documentation](https://clickhouse.com/docs/en/engines/table-engines/"
                        "mergetree-family/replication#replicatedmergetree-parameters) "
                        "for more information. Use `$table_name` to substitute the "
                        "table name.",
        ),
        th.Property(
            "replica_name",
            th.StringType,
            required=False,
            description="The `replica_name` for replicated tables. This is required "
                        "when using any of the replication engines.",
        ),
        th.Property(
            "cluster_name",
            th.StringType,
            required=False,
            description="The cluster to create tables in. This is passed as the "
                        "`clickhouse_cluster` argument when creating a table. "
                        "[Documentation]"
                        "(https://clickhouse.com/docs/en/"
                        "sql-reference/distributed-ddl) "
                        "can be found here.",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            required=False,
            description="The default target database schema name to use for "
                        "all streams.",
        ),
        th.Property(
            "optimize_after",
            th.BooleanType,
            required=False,
            default=False,
            description="Run 'OPTIMIZE TABLE' after data insert. Useful when"
                        "table engine removes duplicate rows.",
        ),
    ).to_dict()

    default_sink_class = ClickhouseSink


if __name__ == "__main__":
    TargetClickhouse.cli()
