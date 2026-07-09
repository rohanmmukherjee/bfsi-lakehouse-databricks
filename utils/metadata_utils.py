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
import inspect

# ----====[MASTER TABLES CONFIG]=====------------------------------------------------------------------------------------------------
TABLE_CONFIG = cfg.TBL_TABLE_CONFIG
TABLE_PROCESS_CONFIG = cfg.TBL_TABLE_PROCESS_CONFIG
INPUT_COLUMN_CONFIG = cfg.TBL_INPUT_COLUMN_CONFIG



# ----====[LOGGIN HELPER FUNC 1 : GET TABLE CONFIG]=====-------------------------------------------------------
@lru_cache(maxsize=128) # 128 means it remembers maxium upto 128distinct table names | Cache memory lives inside notebook/cluster, altering table mid session doesn't affect.
def get_table_config(table_name:str, process_type: str) -> dict:
    """
    Returns the table config for the given table name.
    """
    try:
        if process_type not in ["BRONZE", "SILVER", "GOLD"]:
            raise ValueError(f"[get_table_config] Invalid process_type -'{process_type}'")

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
                    WHERE source_table_name = :current_table AND process_type = :process_type"""
                    ,args={"current_table": table_name,
                           "process_type": process_type}
                    )
        
        config_rows = df.collect()
        if len(config_rows) > 1:
            raise ValueError(f"Multiple configs found for table -'{table_name}' in '{TABLE_CONFIG}'.")
        elif len(config_rows) == 0:
            raise ValueError(f"No config found for table -'{table_name}' in '{TABLE_CONFIG}'.")
        
        data = config_rows[0].asDict()

        return data
    
    except Exception as e:
        logger.error(f"[{inspect.currentframe().f_code.co_name}] FAILED | error={e}")
        raise
# get_table_config.cache_clear() # to clear cache memory when needed.



# ----====[LOGGIN HELPER FUNC 2 : GET TABLE PROCESS CONFIG]=====-------------------------------------------------------
@lru_cache(maxsize=128)
def get_process_config(table_name:str, process_type:str) -> dict:
    """
    Returns the table process config
    """
    try:
        if process_type not in ["BRONZE", "SILVER", "GOLD"]:
            raise ValueError(f"[get_process_config] Invalid process_type -'{process_type}'")

        table_config = get_table_config(table_name, process_type = process_type)
        table_id = table_config["table_id"]
                             
        df = spark.sql(f"""
                    SELECT 
                        table_id,
                        process_type,
                        load_type,
                        depends_on_table_ids,
                        transform_module,
                        write_strategy,
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
        logger.error(f"[{inspect.currentframe().f_code.co_name}] FAILED | error={e}")
        raise



# ----====[LOGGIN HELPER FUNC 3 : GET TABLE COLUMN CONFIG]=====-------------------------------------------------------
@lru_cache(maxsize=128)
def get_input_column_config(table_name:str, process_type:str) -> list[dict]:
    """
    Returns the table column config
    """
    try:
        if process_type not in ["BRONZE", "SILVER", "GOLD"]:
            raise ValueError(f"[get_input_column_config] Invalid process_type -'{process_type}'")

        table_config = get_table_config(table_name, process_type = process_type)
        table_id = table_config["table_id"]

        df = spark.sql(f"""
                            SELECT 
                                column_id,
                                table_id,
                                process_type,
                                source_column_name,
                                target_column_name,
                                data_type,
                                is_nullable,
                                is_pii,
                                column_purpose,
                                transform_expr
                            FROM {INPUT_COLUMN_CONFIG}
                            WHERE table_id = :table_id
                            AND process_type = :process_type
                            AND is_active = true
                            ORDER BY column_id ASC
                        """, args={"table_id": table_id, "process_type": process_type})

        return [row.asDict() for row in df.collect()]

    except Exception as e:
        logger.error(f"[{inspect.currentframe().f_code.co_name}] FAILED | error={e}")
        raise



# ----====[LOGGIN HELPER FUNC 4 : GET TABLE WISE GROUPED COLUMN CONFIG]=====-------------------------------------------------------
@lru_cache(maxsize=128)
def get_column_purpose_groups(table_name: str, process_type: str) -> dict:
    """
    Returns columns grouped by column_purpose for a (table, layer).
    
    Returns dict:
        {
            'business_keys': [...],   # column_purpose = 'KEY'
            'hash_cols':     [...],   # column_purpose = 'ATTRIBUTE'
            'audit_cols':    [...],   # column_purpose = 'AUDIT'
            'derived_cols':  [...]    # column_purpose = 'DERIVED'
        }
    
    Each list contains target_column_name (falls back to source_column_name if NULL).
    Filters is_active = true. Applicable for BRONZE / SILVER / GOLD.
    """
    try:
        if process_type not in ['BRONZE', 'SILVER', 'GOLD']:
            raise ValueError(f"Invalid process_type - '{process_type}'")
        
        config_rows = get_input_column_config(
            table_name   = table_name,
            process_type = process_type
        )
        
        def target_name(r):
            return r['target_column_name'] or r['source_column_name']
        
        groups = {
            'business_keys': [target_name(r) for r in config_rows if r['column_purpose'] == 'KEY'],
            'hash_cols':     [target_name(r) for r in config_rows if r['column_purpose'] == 'ATTRIBUTE'],
            'audit_cols':    [target_name(r) for r in config_rows if r['column_purpose'] == 'AUDIT'],
            'derived_cols':  [target_name(r) for r in config_rows if r['column_purpose'] == 'DERIVED'],
        }
        
        return groups
    
    except Exception as e:
        logger.error(f"[get_column_purpose_groups] FAILED | table={table_name} | layer={process_type} | error={e}")
        raise
    

# if __name__ == "__main__":
    # testing_table_name = "t_Client"
#     processing_table_details = get_table_config(table_name = testing_table_name)
#     print(processing_table_details)
#     processing_table_layer = get_process_config(processing_table_details['source_table_name'], process_type = 'BRONZE')
#     print(processing_table_layer)
    # processing_table_columns = get_input_column_config(testing_table_name, process_type = 'SILVER')
    # print(processing_table_columns)
    # print(get_column_purpose_groups('t_Client', 'SILVER'))   # expect all 4 lists populated
    # print(get_column_purpose_groups('t_Client', 'BRONZE'))   # expect empty lists or raise
    # print(get_column_purpose_groups('t_BogusTable', 'SILVER'))  # expect raise