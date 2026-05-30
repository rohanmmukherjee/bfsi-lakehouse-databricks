# ====== FUNCTION REFERENCE ==============================================================================
#
# get_table_config(table_name) -> dict
#   WHAT : Returns table IDENTITY — source path pattern, target catalog/schema/table,
#          partition cols, replaceWhere template, drift policy. One row per table.
#   USE  : Called once at notebook start to learn "where do I read from, where do I write to".
#   CACHE: @lru_cache — config is immutable within a run; repeated calls are free cache hits.
#
# get_process_config(table_name, process_type) -> dict
#   WHAT : Returns PROCESSING rules for one layer — load_type (FULL/INCREMENTAL),
#          upstream dependencies, transform module. One row per (table, layer).
#   USE  : Called after get_table_config to learn "how do I process this table at this layer".
#   NOTE : Resolves table_name -> table_id internally via get_table_config (free cache hit).
#          process_type must be UPPERCASE: 'BRONZE' / 'SILVER' / 'GOLD'.
#
# WHY TWO FUNCTIONS, NOT ONE:
#   table_config  = 1 row per table   (identity — rarely changes)
#   process_config= N rows per table  (one per layer — evolves as Silver/Gold added)
#   Different cardinality + shared column names (table_id, is_active) => never merge into
#   one dict (key collision). Compose at point-of-use: two named dicts side by side.
#
# ====== HOW IT FLOWS IN A PIPELINE ======================================================================
#
#   Bronze notebook (e.g. ingest t_Client)
#        │
#        ├─(1)─> get_table_config('t_Client')
#        │          returns: {source_path_pattern, target_*, replace_where_template, ...}
#        │          ┌────────────────────────────────────────────────┐
#        │          │ used for: WHERE to read  /  WHERE to write       │
#        │          └────────────────────────────────────────────────┘
#        │
#        ├─(2)─> get_process_config('t_Client', 'BRONZE')
#        │          │  (internally calls get_table_config -> table_id, cache HIT)
#        │          returns: {load_type, depends_on_table_ids, transform_module}
#        │          ┌────────────────────────────────────────────────┐
#        │          │ used for: HOW to read (full vs incremental)      │
#        │          │           WHETHER upstream must finish first     │
#        │          └────────────────────────────────────────────────┘
#        │
#        └─(3)─> ingestion logic reaches into each dict for its own decision
#                (no merge — each decision pulls from one source)
#
#   Same two functions serve Silver & Gold; only process_type changes.
# =========================================================================================================

import sys
PROJECT_ROOT = "/Workspace/Users/rohan.m.mukherjee@gmail.com/bfsi-lakehouse-databricks"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# print(f"[metadata_utils.py] Project root added to sys.path: {PROJECT_ROOT}")



import config.settings as cfg
from datetime import datetime
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError(
        "No active SparkSession. logging_utils must be imported "
        "from an active Databricks notebook or job context."
    )



import logging
import sys
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

from functools import lru_cache

# ----====[MASTER TABLES CONFIG]=====------------------------------------------------------------------------------------------------
TABLE_CONFIG = cfg.TBL_TABLE_CONFIG
TABLE_PROCESS_CONFIG = cfg.TBL_TABLE_PROCESS_CONFIG
INPUT_COLUMN_CONFIG = cfg.TBL_INPUT_COLUMN_CONFIG



# ----====[LOGGIN HELPER FUNC 1 : GET TABLE CONFIG]=====-------------------------------------------------------
@lru_cache(maxsize=128) # 128 means it remembers maxium upto 128distinct table names | Cache memory lives inside notebook/cluster, altering table mid session doesn't affect.
def get_table_config(table_name:str) -> dict:
    """
    Returns the table config for the given table name.
    """
    try:
        df = spark.sql(f"""
                    SELECT 
                        table_id,
                        source_table_name,
                        source_system,
                        source_format,
                        source_path_pattern,
                        target_catalog,
                        target_schema,
                        target_table,
                        partition_cols,
                        replace_where_template,
                        drift_policy,
                        load_priority,
                        is_active
                    FROM {TABLE_CONFIG}
                    WHERE source_table_name = :current_table"""
                    ,args={"current_table": table_name}
                    )
        
        config_rows = df.collect()
        if len(config_rows) > 1:
            raise ValueError(f"Multiple configs found for table -'{table_name}' in '{TABLE_CONFIG}'.")
        elif len(config_rows) == 0:
            raise ValueError(f"No config found for table -'{table_name}' in '{TABLE_CONFIG}'.")
        
        data = config_rows[0].asDict()

        return data
    
    except Exception as e:
        logger.error(f"[get_table_config] FAILED | error={e}")
        raise
# get_table_config.cache_clear() # to clear cache memory when needed.



# ----====[LOGGIN HELPER FUNC 2 : GET PROCESS CONFIG]=====-------------------------------------------------------
@lru_cache(maxsize=128)
def get_process_config(table_name:str, process_type:str) -> dict:
    """
    Returns the table process config
    """
    try:
        if process_type not in ["BRONZE", "SILVER", "GOLD"]:
            raise ValueError(f"Invalid process_type -'{process_type}'")

        table_config = get_table_config(table_name)
        table_id = table_config["table_id"]
                             
        df = spark.sql(f"""
                    SELECT 
                        table_id,
                        process_type,
                        load_type,
                        depends_on_table_ids,
                        transform_module,
                        is_active
                    FROM {TABLE_PROCESS_CONFIG}
                    WHERE table_id = :current_table_id
                        AND process_type = :current_process_type"""
                    ,args={"current_table_id": table_id, "current_process_type": process_type}
                    )
        
        config_rows = df.collect()
        if len(config_rows) > 1:
            raise ValueError(f"Multiple configs found for table -'{table_id}', process_type -'{process_type}' in '{TABLE_PROCESS_CONFIG}'.")
        elif len(config_rows) == 0:
            raise ValueError(f"No config found for table -'{table_id}', process_type -'{process_type}' in '{TABLE_PROCESS_CONFIG}'.")
        
        data = config_rows[0].asDict()

        return data
    
    except Exception as e:
        logger.error(f"[get_process_config] FAILED | error={e}")
        raise



# get_input_column_config 
# This should be applicatable from Silver+ layers, not bronze & should return a dataframe. noyt dictionary


# if __name__ == "__main__":
#     testing_table_name = "t_Client"
#     processing_table_details = get_table_config(table_name = testing_table_name)
#     print(processing_table_details)
#     processing_table_layer = get_process_config(processing_table_details['source_table_name'], process_type = 'BRONZE')
#     print(processing_table_layer)
