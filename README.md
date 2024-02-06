# target-clickhouse

`target-clickhouse` is a Singer target for clickhouse.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-clickhouse
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-clickhouse.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-clickhouse --about --format=markdown
```
-->

| Setting              | Required | Default | Description                                                                                                                                                                                                                                                                                                             |
|:---------------------|:--------:|:-------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| sqlalchemy_url       | False    | None    | The SQLAlchemy connection string for the ClickHouse database. Used if set, otherwise separate settings are used                                                                                                                                                                                                         |
| driver               | False    | http    | Driver type                                                                                                                                                                                                                                                                                                             |
| username             | False    | default | Database user                                                                                                                                                                                                                                                                                                           |
| password             | False    | None    | Username password                                                                                                                                                                                                                                                                                                       |
| host                 | False    | localhost | Database host                                                                                                                                                                                                                                                                                                           |
| port                 | False    |    8123 | Database connection port                                                                                                                                                                                                                                                                                                |
| database             | False    | default | Database name                                                                                                                                                                                                                                                                                                           |
| secure               | False    |       0 | Should the connection be secure                                                                                                                                                                                                                                                                                         |
| verify               | False    |       1 | Should secure connection need to verify SSL/TLS                                                                                                                                                                                                                                                                         |
| engine_type          | False    | None    | The engine type to use for the table.                                                                                                                                                                                                                                                                                   |
| table_name           | False    | None    | The name of the table to write to. Defaults to stream name.                                                                                                                                                                                                                                                             |
| table_path           | False    | None    | The table path for replicated tables. This is required when using any of the replication engines. Check out the [documentation](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication#replicatedmergetree-parameters) for more information. Use `$table_name` to substitute the table name. |
| replica_name         | False    | None    | The `replica_name` for replicated tables. This is required when using any of the replication engines.                                                                                                                                                                                                                   |
| cluster_name         | False    | None    | The cluster to create tables in. This is passed as the `clickhouse_cluster` argument when creating a table. [Documentation](https://clickhouse.com/docs/en/sql-reference/distributed-ddl) can be found here.                                                                                                            |
| default_target_schema| False    | None    | The default target database schema name to use for all streams.                                                                                                                                                                                                                                                         |
| optimize_after       | False    |       0 | Run 'OPTIMIZE TABLE' after data insert                                                                                                                                                                                                                                                                                  |
| optimize_after       | False    |       0 | Run 'OPTIMIZE TABLE' after data insert. Useful whentable engine removes duplicate rows.                                                                                                                                                                                                                                 |
| load_method          | False    | TargetLoadMethods.APPEND_ONLY | The method to use when loading data into the destination. `append-only` will always write all input records whether that records already exists or not. `upsert` will update existing records and insert new records. `overwrite` will delete all existing records and insert all input records.                        |
| stream_maps          | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html).                                                                                                                                                                             |
| stream_map_config    | False    | None    | User-defined config values to be used within map expressions.                                                                                                                                                                                                                                                           |
| flattening_enabled   | False    | None    | 'True' to enable schema flattening and automatically expand nested properties.                                                                                                                                                                                                                                          |
| flattening_max_depth | False    | None    | The max depth to flatten schemas.                                                                                                                                                                                                                                                                                       |

A full list of supported settings and capabilities is available by running: `target-clickhouse --about`

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your target requires special access on the destination system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `target-clickhouse` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-clickhouse --version
target-clickhouse --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-clickhouse --config /path/to/target-clickhouse-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Start the Clickhouse container

In order to run the tests locally, you must have a Docker daemon running on your host machine.

You can start the Clickhouse container by running:
```
./docker_run_clickhouse.sh
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-clickhouse` CLI interface directly using `poetry run`:

```bash
poetry run target-clickhouse --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-clickhouse
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-clickhouse --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-clickhouse
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
