from __future__ import annotations

import contextlib
import typing
from typing import TYPE_CHECKING

import sqlalchemy.types
from clickhouse_sqlalchemy import (
    Table,
)
from clickhouse_sqlalchemy import (
    types as clickhouse_sqlalchemy_types,
)
from pkg_resources import get_distribution, parse_version
from singer_sdk import typing as th
from singer_sdk.connectors import SQLConnector
from sqlalchemy import Column, MetaData, create_engine

from target_clickhouse.engine_class import SupportedEngines, create_engine_wrapper

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class ClickhouseConnector(SQLConnector):
    """Clickhouse Meltano Connector.

    Inherits from `SQLConnector` class, overriding methods where needed
    for Clickhouse compatibility.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for clickhouse.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return super().get_sqlalchemy_url(config)

        if config["driver"] == "http":
            if config["secure"]:
                secure_options = f"protocol=https&verify={config['verify']}"

                if not config["verify"]:
                    # disable urllib3 warning
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            else:
                secure_options = "protocol=http"
        else:
            secure_options = f"secure={config['secure']}&verify={config['verify']}"
        return (
            f"clickhouse+{config['driver']}://{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/"
            f"{config['database']}?{secure_options}"
        )

    def create_engine(self) -> Engine:
        """Create a SQLAlchemy engine for clickhouse."""
        return create_engine(self.get_sqlalchemy_url(self.config))

    @contextlib.contextmanager
    def _connect(self) -> typing.Iterator[sqlalchemy.engine.Connection]:
        # patch to overcome error in sqlalchemy-clickhouse driver
        if self.config.get("driver") == "native":
            kwargs = {"stream_results": True, "max_row_buffer": 1000}
        else:
            kwargs = {"stream_results": True}
        with self._engine.connect().execution_options(**kwargs) as conn:
            yield conn

    def to_sql_type(self, jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        sql_type = th.to_sql_type(jsonschema_type)

        # Clickhouse does not support the DECIMAL type without providing precision,
        # so we need to use the FLOAT type.
        if type(sql_type) == sqlalchemy.types.DECIMAL:
            sql_type = typing.cast(
                sqlalchemy.types.TypeEngine, sqlalchemy.types.FLOAT(),
            )
        elif type(sql_type) == sqlalchemy.types.INTEGER:
            sql_type = typing.cast(
                sqlalchemy.types.TypeEngine, clickhouse_sqlalchemy_types.Int64(),
            )

        return sql_type

    def create_empty_table(
            self,
            full_table_name: str,
            schema: dict,
            primary_keys: list[str] | None = None,
            partition_keys: list[str] | None = None,
            as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> None:
        """Create an empty target table, using Clickhouse Engine.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        if as_temp_table:
            msg = "Temporary tables are not supported."
            raise NotImplementedError(msg)

        _ = partition_keys  # Not supported in generic implementation.

        _, _, table_name = self.parse_full_table_name(full_table_name)

        # If config table name is set, then use it instead of the table name.
        if self.config.get("table_name"):
            table_name = self.config.get("table_name")

        # Do not set schema, as it is not supported by Clickhouse.
        # Get the version of sqlalchemy
        sqlalchemy_version = get_distribution("sqlalchemy").version
        parsed_version = parse_version(sqlalchemy_version)
        if parsed_version < parse_version("2.0"):
            # Code for sqlalchemy 1.0 compatibility.
            meta = MetaData(schema=None, bind=self._engine)
        else:
            meta = MetaData(schema=None)

        columns: list[Column] = []
        primary_keys = primary_keys or []

        # If config engine type is set, then use it instead of the default engine type.
        if self.config.get("engine_type"):
            engine_type = self.config.get("engine_type")
        else:
            engine_type = SupportedEngines.MERGE_TREE

        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                ),
            )

        table_engine = create_engine_wrapper(
            engine_type=engine_type,
            primary_keys=primary_keys,
            table_name=table_name,
            config=self.config,
        )

        table_args = {}
        if self.config.get("cluster_name"):
            table_args["clickhouse_cluster"] = self.config.get("cluster_name")

        _ = Table(table_name, meta, *columns, table_engine, **table_args)
        meta.create_all(self._engine)

    def prepare_schema(self, _: str) -> None:
        """Create the target database schema.

        In Clickhouse, a schema is a database, so this method is a no-op.

        Args:
            schema_name: The target schema name.
        """
        return

    def prepare_column(
            self,
            full_table_name: str,
            column_name: str,
            sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
        """
        if not self.column_exists(full_table_name, column_name):
            self._create_empty_column(
                full_table_name=full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )
            return

        with contextlib.suppress(NotImplementedError):
            self._adapt_column_type(
                full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )

    @staticmethod
    def get_column_add_ddl(
            table_name: str,
            column_name: str,
            column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                column_type,
            ),
        )
        return sqlalchemy.DDL(
            (
                "ALTER TABLE %(table_name)s ADD COLUMN IF NOT EXISTS "
                "%(create_column_clause)s"
            ),
            {
                "table_name": table_name,
                "create_column_clause": create_column_clause,
            },
        )

    def get_column_alter_ddl(
            self,
            table_name: str,
            column_name: str,
            column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the alter column DDL statement.

        Overrides the static method in the base class to support ON CLUSTER.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        if self.config.get("cluster_name"):
            return sqlalchemy.DDL(
                (
                    "ALTER TABLE %(table_name)s ON CLUSTER %(cluster_name)s "
                    "MODIFY COLUMN %(column_name)s %(column_type)s",
                ),
                {
                    "table_name": table_name,
                    "column_name": column_name,
                    "column_type": column_type,
                    "cluster_name": self.config.get("cluster_name"),
                },
            )
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s MODIFY COLUMN %(column_name)s %(column_type)s",
            {
                "table_name": table_name,
                "column_name": column_name,
                "column_type": column_type,
            },
        )
