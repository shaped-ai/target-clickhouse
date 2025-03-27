from __future__ import annotations

from singer_sdk.target_base import SQLTarget

from target_clickhouse.sinks import (
    ClickhouseSink,
)


class TargetClickhouse(SQLTarget):
    """SQL-based target for Clickhouse."""

    name = "target-clickhouse"

    #     # connection properties
    #     th.Property(
    #         "sqlalchemy_url",
    #         th.StringType,
    #                     "Used if set, otherwise separate settings are used",
    #     ),
    #     th.Property(
    #         "driver",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "username",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "password",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "host",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "port",
    #         th.IntegerType,
    #     ),
    #     th.Property(
    #         "database",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "secure",
    #         th.BooleanType,
    #     ),
    #     th.Property(
    #         "verify",
    #         th.BooleanType,
    #     ),

    #     # other settings
    #     th.Property(
    #         "engine_type",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "table_name",
    #         th.StringType,
    #     ),
    #     th.Property(
    #         "table_path",
    #         th.StringType,
    #                     "using any of the replication engines. Check out the "
    #                     "for more information. Use `$table_name` to substitute the "
    #                     "table name.",
    #     ),
    #     th.Property(
    #         "replica_name",
    #         th.StringType,
    #                     "when using any of the replication engines.",
    #     ),
    #     th.Property(
    #         "cluster_name",
    #         th.StringType,
    #                     "`clickhouse_cluster` argument when creating a table. "
    #                     "can be found here.",
    #     ),
    #     th.Property(
    #         "default_target_schema",
    #         th.StringType,
    #                     "all streams.",
    #     ),
    #     th.Property(
    #         "optimize_after",
    #         th.BooleanType,
    #                     "table engine removes duplicate rows.",
    #     ),
    #     th.Property(
    #         "order_by_keys",
    #                     "ordering.",
    #     ),
    # ).to_dict()

    default_sink_class = ClickhouseSink


if __name__ == "__main__":
    TargetClickhouse.cli()
