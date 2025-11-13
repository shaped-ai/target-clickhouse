import pytest
import sqlalchemy

from target_clickhouse.target import TargetClickhouse
from target_clickhouse.sinks import ClickhouseSink


CONFIG = {
    "driver": "http",
    "host": "localhost",
    "port": 18123,
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
    # Use a specific engine so we can assert the returned value explicitly.
    "engine_type": "ReplacingMergeTree",
}


@pytest.fixture(scope="module")
def sink():
    target = TargetClickhouse(config=CONFIG, parse_env_config=False)
    return ClickhouseSink(
        target=target,
        stream_name="test_engine_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )


@pytest.fixture(scope="module")
def ensure_clickhouse_running(sink):  # noqa: D401
    """Skip tests gracefully if ClickHouse is not reachable."""
    try:
        with sink.connector._connect() as conn:  # noqa: SLF001
            conn.execute(sqlalchemy.text("SELECT 1"))
    except Exception as exc:  # broad catch to skip on any connection issue
        pytest.skip(f"ClickHouse not available: {exc}")


def test_clickhouse_table_engine_returns_engine(ensure_clickhouse_running, sink):
    schema = {"properties": {"id": {"type": "integer"}}}
    full_table_name = sink.full_table_name

    # Ensure clean state: drop table if exists
    with sink.connector._connect() as conn:  # noqa: SLF001
        conn.execute(
            sqlalchemy.text(
                f"DROP TABLE IF EXISTS {sink.table_name}",
            )
        )

    sink.connector.create_empty_table(
        full_table_name=full_table_name,
        schema=schema,
        primary_keys=["id"],
    )

    engine_name = sink.clickhouse_table_engine()

    assert engine_name == "ReplacingMergeTree", (
        f"Expected 'ReplacingMergeTree' engine, got {engine_name!r}"
    )


def test_clickhouse_table_engine_returns_none_for_missing_table(ensure_clickhouse_running, sink):
    # Use a fresh sink pointing to a table that does not exist
    target = TargetClickhouse(config=CONFIG, parse_env_config=False)
    missing_sink = ClickhouseSink(target=target, stream_name="non_existent_table_xyz")

    # Ensure table truly does not exist (DROP if present)
    with missing_sink.connector._connect() as conn:  # noqa: SLF001
        conn.execute(
            sqlalchemy.text(
                f"DROP TABLE IF EXISTS {missing_sink.table_name}",
            )
        )

    assert missing_sink.clickhouse_table_engine() is None

