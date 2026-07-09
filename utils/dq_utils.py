import sys
PROJECT_ROOT = "/Workspace/Users/rohan.m.mukherjee@gmail.com/bfsi-lakehouse-databricks"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# print(f"[dq_utils.py] Project root added to sys.path: {PROJECT_ROOT}")



import config.settings as cfg
import uuid
from datetime import datetime
from enum import Enum
from functools import reduce
from operator import or_
import re
import inspect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window
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


# ----====[LOGGIN HELPER FUNC 1 : To config schema enforcement]=====-----------------------------------------------
def enforce_schema(table_name: str, df, config_rows, drift_policy: str):
    try:
        df_valid, df_quarantine = None, None
        drift_events = []
        action_map = {                              # moved to top — used in Step 1 + 1.5
            'STRICT'    : 'REJECTED',
            'QUARANTINE': 'QUARANTINED',
            'EVOLVE'    : 'AUTO_EVOLVED'
        }

        # ------: STEP 1 — Missing columns check
        inp_df_columns = df.columns
        config_columns = [
            row['source_column_name']
            for row in config_rows
            if row['source_column_name'] is not None
        ]
        missing_columns = list(set(config_columns) - set(inp_df_columns))

        if missing_columns:
            for col_name in missing_columns:
                drift_events.append({
                    'column_name'      : col_name,
                    'column_data_type' : None,
                    'event_type'       : 'MISSING',
                    'action_taken'     : action_map.get(drift_policy, 'REJECTED'),
                    'reason'           : "MISSING_MANDATORY_COLUMNS"
                })

            if drift_policy == 'STRICT':
                raise ValueError(
                    f"[enforce_schema] Mandatory config columns missing for table: {table_name}. "
                    f"Missing: {missing_columns}"
                )
            elif drift_policy == 'QUARANTINE':  # For QUARANTINE, if any mandatory column is missing then entire df will be Quarntined
                return None, df, drift_events
            elif drift_policy == 'EVOLVE':
                for col_name in missing_columns:
                    df = df.withColumn(col_name, F.lit(None))
                return df,None, drift_events    # For Evolve, adding missing columns will NULL, marking it as valid_df & processing.

        # ------: STEP 1.5 — Type comparison check (TYPE_CHANGED detection) (TYPE CHANGED ALLOWED FOR STRICT)
        for row in config_rows:
            src_col       = row['source_column_name']
            expected_type = row['data_type']

            if src_col is None:                     # skip DERIVED
                continue
            if src_col not in inp_df_columns:       # skip missing — handled above
                continue
            if expected_type is None:               # skip if no type configured
                continue

            actual_type = dict(df.dtypes).get(src_col)

            if re.sub(r'\s+', '', actual_type.lower()) != re.sub(r'\s+', '', expected_type.lower()):
                drift_events.append({
                    'column_name'      : src_col,
                    'column_data_type' : actual_type,
                    'event_type'       : 'TYPE_CHANGED',
                    'action_taken'     : action_map.get(drift_policy, 'REJECTED'),
                    'reason'           : "MANDATORY_COLUMNS_TYPE_CAST_FAILED"
                })

        # ------: STEP 2 — Build select expressions
        regular_col_exprs = [
            F.expr(f"try_cast(`{i['source_column_name']}` AS {i['data_type']})")
            .alias(i['target_column_name'] or i['source_column_name'])
            for i in config_rows
            if i['source_column_name'] is not None
        ]
        derived_col_exprs = [
            F.expr(i['transform_expr']).alias(i['target_column_name'])
            for i in config_rows
            if i['column_purpose'] == 'DERIVED'
            and i['transform_expr'] is not None
        ]
        carry_audit_cols = ['dt'] + cfg.AUDIT_COLS.as_list()
        carry_audit_cols = [c for c in carry_audit_cols if c in df.columns]   # safety filter
        final_exprs      = regular_col_exprs + derived_col_exprs + carry_audit_cols

        # ------: STEP 3 — Apply select
        result_df = df.select(*final_exprs)

        # ------: STEP 4 — NOT NULL columns (exclude DERIVED)
        not_null_columns = [
            i['target_column_name'] or i['source_column_name']
            for i in config_rows
            if i['is_nullable'] is False
            and i['column_purpose'] != 'DERIVED'
        ]

        # ------: STEP 5 — Split valid / quarantine
        if not_null_columns:
            filter_expr    = [F.col(c).isNull() for c in not_null_columns]
            null_condition = reduce(or_, filter_expr)
            df_valid       = result_df.filter(~null_condition)
            df_quarantine  = result_df.filter(null_condition)
        else:
            df_valid      = result_df
            df_quarantine = None

        # ------: STEP 6 — Return            
        return df_valid, df_quarantine, drift_events

    except Exception as e:
        logger.error(f"[enforce_schema] FAILED | error={e}")
        raise



# ----====[LOGGIN HELPER FUNC 2 : To write quarantine dataframes to the quarantine table]=====-----------------------------------------------
def write_to_quarantine(df_quarantine, source_table_name : str, run_id : str,log_id : str,run_dt : str,drift_events : list) -> dict:
    try:
    
        # ------: STEP 1 — Empty Check for Quarantine Dataframe
        if df_quarantine is None or df_quarantine.isEmpty():
            logger.info(f"[write_to_quarantine] No quarantine rows for {source_table_name} | run_dt={run_dt}")
            return {
                'is_quarantined'   : False,
                'quarantine_table' : source_table_name,
                'run_id'           : run_id,
                'log_id'           : log_id,
                'run_dt'           : run_dt,
                'quarantine_count' : 0
            }
        
        # ------: STEP 1.5 — Derive quarantine reason
        if not drift_events:
            quarantine_reason = 'NULL_IN_NOT_NULL_COLUMN'
        else:
            quarantine_reason = next(   # first dict in drift_events that has a reason key" | fallback to NULL_IN_NOT_NULL_COLUMN if none found
                (event.get('reason') for event in drift_events if event.get('reason')),
                'NULL_IN_NOT_NULL_COLUMN'
            )

        # ------: STEP 2 — Stamp 4 trace columns
        df_quarantine = (
            df_quarantine
            .withColumn('_quarantine_reason', F.lit(quarantine_reason))
            .withColumn('_quarantined_at',    F.current_timestamp())
            .withColumn('_run_id',            F.lit(run_id))
            .withColumn('_log_id',            F.lit(log_id))
        )

        # ------: STEP 3 — Build target
        target = f"bfsi_lakehouse.quarantine.{source_table_name}"

        # ------: STEP 4 — Write
        table_exists = spark.catalog.tableExists(target)
        if not table_exists:
            (
                df_quarantine.write
                .format("delta")
                .mode("overwrite")          # plain overwrite, no replaceWhere needed
                .partitionBy("dt")
                .saveAsTable(target)
            )
        else:
            (
                df_quarantine.write
                .format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"dt = '{run_dt}'")
                .partitionBy("dt")
                .saveAsTable(target)
            )

        # ------: STEP 5 — Return status
        quarantine_count = df_quarantine.count()
        logger.info(f"[write_to_quarantine] {source_table_name} | quarantine_count={quarantine_count} | target={target} | run_dt={run_dt}")
        
        return {
            'is_quarantined'   : True,
            'quarantine_table' : source_table_name,
            'run_id'           : run_id,
            'log_id'           : log_id,
            'run_dt'           : run_dt,
            'quarantine_count' : quarantine_count
        }

    except Exception as e:
        logger.error(f"[write_to_quarantine] FAILED | error={e}")
        raise



# ----====[LOGGIN HELPER FUNC 3 : Handle duplicates for Merge =====-----------------------------------------------
def dedup_for_merge(
    df,                          # incoming Silver dataframe (post enforce_schema)
    business_keys: list,         # from col_groups['business_keys']
    order_by_col: str,           # e.g. 'LastUpdatedAt'
    hash_cols: list              # from col_groups['hash_cols']
) -> DataFrame:
    """
    Adds _row_hash column and deduplicates to one row per business_key (latest wins).
    Output is MERGE-ready: unique on business_keys, fingerprinted via _row_hash.
    """
    try:

        # ------: STEP 1 — Inputes Validation
        if not business_keys:
            raise ValueError(f"[{inspect.currentframe().f_code.co_name}] No business keys provided.")
        if not order_by_col:
            raise ValueError(f"[{inspect.currentframe().f_code.co_name}] No order_by_col provided.")
        if not hash_cols:
            raise ValueError(f"[{inspect.currentframe().f_code.co_name}] No hash_cols provided.")

        missing_business_keys = [col for col in business_keys if col not in df.columns]
        if missing_business_keys:
            raise ValueError(f"[dedup_for_merge] Business key columns missing in df: {missing_business_keys}")

        if order_by_col not in df.columns:
            raise ValueError(f"[dedup_for_merge] order_by_col '{order_by_col}' not in df")

        missing_hash_cols = [col for col in hash_cols if col not in df.columns]
        if missing_hash_cols:
            raise ValueError(f"[dedup_for_merge] Hash columns missing in df: {missing_hash_cols}")


        # ------: STEP 2 — Build hash expression
        sorted_hash_cols = sorted(hash_cols)
        hash_input_cols = [F.coalesce(F.col(col).cast('string'), F.lit(''))
                            for col in sorted_hash_cols]
        hash_expr = F.sha2(
            F.concat_ws('||', *hash_input_cols),
            256
        )
        # print(f"hash_input_cols ---> {hash_input_cols}")
        # print(f"hash_expr ---> {hash_expr}")

        # ------: STEP 3 — Add hash column
        df_with_hash = df.withColumn("_row_hash",hash_expr)
        # print(df_with_hash['OurBranchID','ClientID','_row_hash'].head(5))

        # ------: STEP 4 — Build Window
        w = Window.partitionBy(*business_keys).orderBy(F.col(order_by_col).desc_nulls_last())

        # ------: STEP 5 — Apply row_number + filter
        df_dedup = (
            df_with_hash
            .withColumn('_rn', F.row_number().over(w))
            .filter(F.col('_rn') == 1)
            .drop('_rn')
        )

        logger.info(
            f"[dedup_for_merge] "
            + f"\n{' ' * 15} >>> business_keys = {business_keys}"
            + f"\n{' ' * 15} >>> order_by_col  = {order_by_col}"
            + f"\n{' ' * 15} >>> hash_cols     = {len(sorted_hash_cols)} columns"
        )

        return df_dedup
    
    except Exception as e:
        logger.error(f"[dedup_for_merge] FAILED | error={e}")
        raise




if __name__ == "__main__":
    import config.settings as cfg
    from utils.metadata_utils import (get_table_config,get_input_column_config,get_column_purpose_groups)

    bronze_df = spark.table('bfsi_lakehouse.bronze.t_client').filter("dt = '2024-01-02'")
    config_columns_details = get_input_column_config(table_name = 't_Client', process_type = 'SILVER')

    table_config_details = get_table_config(table_name = 't_Client', process_type = 'SILVER')
    drift_policy = table_config_details['drift_policy']
    df_valid,df_quarantine,drift_events = enforce_schema(table_name = 't_Client',
                                                         df = bronze_df,
                                                         config_rows = config_columns_details,
                                                         drift_policy = drift_policy)
    write_to_quarantine(df_quarantine = df_quarantine,
                        source_table_name = 't_Client',
                        run_id = '12345',
                        log_id = '12345',
                        run_dt = '2024-01-02',
                        drift_events = drift_events)

    silver_purporse_groups = get_column_purpose_groups(table_name = 't_Client', process_type = 'SILVER')
    silver_business_purpose_groups = silver_purporse_groups['business_keys']
    silver_hash_purpose_groups = silver_purporse_groups['hash_cols']
    silver_audit_purpose_groups = silver_purporse_groups['audit_cols']
    silver_derived_purpose_groups = silver_purporse_groups['derived_cols']

    df_dedup = dedup_for_merge(
        df = df_valid,
        business_keys = silver_business_purpose_groups,
        order_by_col = 'LastUpdatedAt',
        hash_cols = silver_hash_purpose_groups
    )

    print(df_dedup.head(5))
