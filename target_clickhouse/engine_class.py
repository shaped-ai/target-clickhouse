from enum import Enum
from string import Template
from typing import List, Optional

from clickhouse_sqlalchemy import engines
from sqlalchemy import func


class SupportedEngines(str, Enum):
    MERGE_TREE = "MergeTree"
    REPLACING_MERGE_TREE = "ReplacingMergeTree"
    SUMMING_MERGE_TREE = "SummingMergeTree"
    AGGREGATING_MERGE_TREE = "AggregatingMergeTree"
    REPLICATED_MERGE_TREE = "ReplicatedMergeTree"
    REPLICATED_REPLACING_MERGE_TREE = "ReplicatedReplacingMergeTree"
    REPLICATED_SUMMING_MERGE_TREE = "ReplicatedSummingMergeTree"
    REPLICATED_AGGREGATING_MERGE_TREE = "ReplicatedAggregatingMergeTree"


ENGINE_MAPPING = {
    SupportedEngines.MERGE_TREE: engines.MergeTree,
    SupportedEngines.REPLACING_MERGE_TREE: engines.ReplacingMergeTree,
    SupportedEngines.SUMMING_MERGE_TREE: engines.SummingMergeTree,
    SupportedEngines.AGGREGATING_MERGE_TREE: engines.AggregatingMergeTree,
    SupportedEngines.REPLICATED_MERGE_TREE: engines.ReplicatedMergeTree,
    SupportedEngines.REPLICATED_REPLACING_MERGE_TREE:
        engines.ReplicatedReplacingMergeTree,
    SupportedEngines.REPLICATED_SUMMING_MERGE_TREE: engines.ReplicatedSummingMergeTree,
    SupportedEngines.REPLICATED_AGGREGATING_MERGE_TREE:
        engines.ReplicatedAggregatingMergeTree,
}


def is_supported_engine(engine_type):
    return engine_type in SupportedEngines.__members__.values()


def get_engine_class(engine_type):
    return ENGINE_MAPPING.get(engine_type)


def create_engine_wrapper(
    engine_type,
    primary_keys: List[str],
    table_name: str,
    config: Optional[dict] = None,
):
    # check if engine type is in supported engines
    if is_supported_engine(engine_type) is False:
        msg = f"Engine type {engine_type} is not supported."
        raise ValueError(msg)

    engine_args: dict = {}
    if len(primary_keys) > 0:
        engine_args["primary_key"] = primary_keys
    else:
        # If no primary keys are specified,
        # then Clickhouse expects the data to be indexed on all fields via tuple().
        engine_args["order_by"] = func.tuple()

    if config is not None:
        if engine_type in (
            SupportedEngines.REPLICATED_MERGE_TREE,
            SupportedEngines.REPLICATED_REPLACING_MERGE_TREE,
            SupportedEngines.REPLICATED_SUMMING_MERGE_TREE,
            SupportedEngines.REPLICATED_AGGREGATING_MERGE_TREE,
        ):
            table_path: Optional[str] = config.get("table_path")
            if table_path is not None:
                if "$" in table_path:
                    table_path = Template(table_path).substitute(table_name=table_name)
                engine_args["table_path"] = table_path
            else:
                msg = "Table path (table_path) is not defined."
                raise ValueError(msg)
            replica_name: Optional[str] = config.get("replica_name")
            if replica_name is not None:
                engine_args["replica_name"] = replica_name
            else:
                msg = "Replica name (replica_name) is not defined."
                raise ValueError(msg)

        engine_class = get_engine_class(engine_type)

    return engine_class(**engine_args)
