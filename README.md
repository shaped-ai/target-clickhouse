# target-clickhouse

`target-clickhouse` is a Singer target for ClickHouse, built with the [Meltano Target SDK](https://sdk.meltano.com). This project is a fork of [shaped-ai/target-clickhouse](https://github.com/shaped-ai/target-clickhouse) with additional features and active maintenance.

## Fork Information

This fork was created to provide:

- Active maintenance and updates
- Additional features and improvements
- Regular releases and bug fixes

While we maintain compatibility where possible, some behaviors may differ from the original project. Please refer to the release notes for details on changes and improvements.

## Features

- Supports various ClickHouse table engines including MergeTree family
- Handles data replication and distributed tables
- Supports schema changes and data type conversions
- Provides flexible configuration for connection and table settings

## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/dlouseiro/target-clickhouse.git@main
```

Or install a specific version:

```bash
pipx install git+https://github.com/dlouseiro/target-clickhouse.git@v0.2.5
```

## Development

### Setup

1. Clone the repository:

```bash
git clone https://github.com/dlouseiro/target-clickhouse.git
cd target-clickhouse
```

2. Install dependencies using Poetry:

```bash
poetry install --all-extras
```

## Configuration

### Connection Settings

| Setting        | Required | Default   | Description                                                                 |
| :------------- | :------- | :-------- | :-------------------------------------------------------------------------- |
| sqlalchemy_url | False    | None      | SQLAlchemy connection string. If set, other connection settings are ignored |
| driver         | False    | http      | Driver type: `http`, `native`, or `asynch`                                  |
| username       | False    | default   | Database user                                                               |
| password       | False    | None      | User password                                                               |
| host           | False    | localhost | Database host                                                               |
| port           | False    | 8123      | Database port (8123 for HTTP, 9000 for native)                              |
| database       | False    | default   | Database name                                                               |
| secure         | False    | false     | Enable secure connection                                                    |
| verify         | False    | true      | Verify SSL/TLS certificates                                                 |

### Table Settings

| Setting               | Required | Default | Description                                                 |
| :-------------------- | :------- | :------ | :---------------------------------------------------------- |
| engine_type           | False    | None    | Table engine type (e.g., MergeTree, ReplacingMergeTree)     |
| table_name            | False    | None    | Target table name (defaults to stream name)                 |
| table_path            | False    | None    | Table path for replicated tables (required for replication) |
| replica_name          | False    | None    | Replica name for replicated tables                          |
| cluster_name          | False    | None    | Cluster name for distributed tables                         |
| default_target_schema | False    | None    | Default target schema/database for all streams              |
| optimize_after        | False    | false   | Run OPTIMIZE TABLE after insert (useful for deduplication)  |
| order_by_keys         | False    | None    | Columns to order by (required for MergeTree engines)        |

### Data Loading Settings

| Setting            | Required | Default     | Description                                                  |
| :----------------- | :------- | :---------- | :----------------------------------------------------------- |
| load_method        | False    | append-only | Data loading method: `append-only`, `upsert`, or `overwrite` |
| stream_maps        | False    | None        | Stream maps configuration for data transformation            |
| stream_map_config  | False    | None        | User-defined values for stream map expressions               |
| flattening_enabled | False    | None        | Enable automatic flattening of nested properties             |

### Environment Variables

All configuration settings can be provided using environment variables with the prefix `TAP_CLICKHOUSE_`. For example:

```bash
export TAP_CLICKHOUSE_HOST=localhost
export TAP_CLICKHOUSE_PORT=8123
export TAP_CLICKHOUSE_DATABASE=my_database
```

For secure credentials, it's recommended to use environment variables instead of storing them in configuration files:

```bash
export TAP_CLICKHOUSE_USERNAME=my_user
export TAP_CLICKHOUSE_PASSWORD=my_password
```

## Usage Examples

### Basic Configuration

Here's a basic example using HTTP connection:

```yaml
plugins:
  loaders:
    - name: target-clickhouse
      config:
        host: localhost
        port: 8123
        database: my_database
```

Or using SQLAlchemy URL:

```yaml
plugins:
  loaders:
    - name: target-clickhouse
      config:
        sqlalchemy_url: "clickhouse+http://default:@localhost:8123/my_database"
```

### Using MergeTree Engine

Example configuration using the MergeTree engine with ordering:

```yaml
plugins:
  loaders:
    - name: target-clickhouse
      config:
        engine_type: "MergeTree"
        order_by_keys: ["id", "created_at"]
```

### Replicated Tables

Example configuration for replicated tables:

```yaml
plugins:
  loaders:
    - name: target-clickhouse
      config:
        engine_type: "ReplicatedMergeTree"
        table_path: "/clickhouse/tables/{shard}/my_database/$table_name"
        replica_name: "replica1"
        order_by_keys: ["id"]
```

### Data Loading Methods

Example of using different loading methods:

```yaml
plugins:
  loaders:
    - name: target-clickhouse
      config:
        # For append-only (default)
        load_method: "append-only"

        # For upsert (requires primary key)
        load_method: "upsert"
        engine_type: "ReplacingMergeTree"
        order_by_keys: ["id"]
        optimize_after: true

        # For overwrite
        load_method: "overwrite"
```

## Supported Table Engines

- `MergeTree`
- `ReplacingMergeTree`
- `SummingMergeTree`
- `AggregatingMergeTree`
- `ReplicatedMergeTree`
- `ReplicatedReplacingMergeTree`
- `ReplicatedSummingMergeTree`
- `ReplicatedAggregatingMergeTree`

For more information about ClickHouse table engines, refer to the [official documentation](https://clickhouse.com/docs/en/engines/table-engines).

### Testing

The test suite includes tests for core functionality and validation. The test environment is automatically set up and torn down as needed.

```bash
# Run all tests
poetry run pytest
```

### Code Quality

This project uses pre-commit hooks to ensure code quality:

- YAML linting
- Code formatting with Prettier
- Python linting with Ruff
- Type checking with MyPy

```bash
# Install pre-commit hooks
poetry run pre-commit install

# Run checks manually
poetry run pre-commit run --all-files
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes and commit them: `git commit -m 'Add feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

Make sure to:

- Add tests for any new features
- Update documentation as needed
- Follow the existing code style
- Run pre-commit hooks before committing

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Additional Information

For a complete list of settings and capabilities:

```bash
target-clickhouse --about --format=markdown
```
