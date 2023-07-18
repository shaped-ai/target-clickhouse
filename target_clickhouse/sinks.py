
from __future__ import annotations

from singer_sdk.sinks import SQLSink

from target_clickhouse.connector import ClickhouseConnector


class ClickhouseSink(SQLSink):
    """Clickhouse Target Sink Class."""

    connector_class = ClickhouseConnector
