from clickhouse_sqlalchemy import engines
from enum import Enum


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
    SupportedEngines.REPLICATED_REPLACING_MERGE_TREE: engines.ReplicatedReplacingMergeTree,
    SupportedEngines.REPLICATED_SUMMING_MERGE_TREE: engines.ReplicatedSummingMergeTree,
    SupportedEngines.REPLICATED_AGGREGATING_MERGE_TREE: engines.ReplicatedAggregatingMergeTree,
}


def is_supported_engine(engine_type):
    return engine_type in SupportedEngines.__members__.values()


def get_engine_class(engine_type):
    return ENGINE_MAPPING.get(engine_type)


def create_engine_wrapper(engine_type, primary_keys, config: dict | None = None):
    # check if engine type is in supported engines
    if is_supported_engine(engine_type) is False:
        raise ValueError(f"Engine type {engine_type} is not supported.")

    engine_args = {"primary_key": primary_keys}
    if config is not None:
        if engine_type in (
            SupportedEngines.REPLICATED_MERGE_TREE,
            SupportedEngines.REPLICATED_REPLACING_MERGE_TREE,
            SupportedEngines.REPLICATED_SUMMING_MERGE_TREE,
            SupportedEngines.REPLICATED_AGGREGATING_MERGE_TREE,
        ):
            if config.get("table_path"):
                engine_args["table_path"] = config.get("table_path")
            if config.get("replica_name"):
                engine_args["replica_name"] = config.get("replica_name")

        engine_class = get_engine_class(engine_type)

    return engine_class(**engine_args)
