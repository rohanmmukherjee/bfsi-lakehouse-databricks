# dbutils.library.restartPython()

import sys
PROJECT_ROOT = "/Workspace/Users/rohan.m.mukherjee@gmail.com/bfsi-lakehouse-databricks"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# print(f"✓ Project root added to sys.path: {PROJECT_ROOT}")


from pyspark.sql import SparkSession
spark = globals().get('spark', None)
if spark is None:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            "No active SparkSession found. Please run this script from a Databricks notebook."
        )

import config.settings as cfg
from utils.audit_utils import (
    start_etl_run,
    log_table_start,
    log_table_end,
    end_etl_run,
    write_schema_drift_log
)

from utils.metadata_utils import (
    get_table_config,
    get_process_config,
    get_input_column_config,
    get_column_purpose_groups
)

from utils.dq_utils import (
    enforce_schema,
    write_to_quarantine,
    dedup_for_merge
)

from utils.merge_utils import (
    scd2_merge,
    upsert_merge,
    insert_only_merge
)

import uuid
from datetime import datetime
from pyspark.sql import functions as F
from pathlib import Path

import logging
import sys
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ----====[MASTER TABLES CONFIG]=====------------------------------------------------------------------------------------------------
TABLE_CONFIG = cfg.TBL_TABLE_CONFIG
TABLE_PROCESS_CONFIG = cfg.TBL_TABLE_PROCESS_CONFIG
INPUT_COLUMN_CONFIG = cfg.TBL_INPUT_COLUMN_CONFIG

# ----====[HELPER FUNC 1 : transform_one_table]=====-----------------------------------------------
def process_one_table_silver (spark,process_run_id, process_log_id, table_name: str, run_dt) -> dict:
    """
    Stateless Silver-layer worker for one table, one run_dt.

    Reads Bronze (partition = run_dt), applies schema enforcement +
    DQ split via enforce_schema(), dedups via dedup_for_merge(), and
    writes to Silver using the write_strategy resolved from
    table_process_config (SCD2 / upsert / append-only MERGE) for this
    table_name. Dynamic transform dispatch via transform_module path.

    Used by the Silver orchestrator to process tables one by one.

    Returns:
        dict with keys:
            log_id, status, table_name,
            rows_read, rows_written,
            version_before, version_after,
            error
    """

    try:
        # ------: Step 1 : Get table, column, drift policy & purpose_groups configs
        table_config_details = get_table_config(table_name = table_name, process_type = 'SILVER')
        config_columns_details = get_input_column_config(table_name = table_name, process_type = 'SILVER')
        drift_policy = table_config_details['drift_policy']

        silver_purporse_groups = get_column_purpose_groups(table_name = table_name, process_type = 'SILVER')
        silver_business_purpose_groups = silver_purporse_groups['business_keys']
        silver_hash_purpose_groups = silver_purporse_groups['hash_cols']
        silver_audit_purpose_groups = silver_purporse_groups['audit_cols']
        silver_derived_purpose_groups = silver_purporse_groups['derived_cols']


        # ------: Step 2 : Capture Silver target delta version_before
        silver_fqn = cfg.fqn(schema=cfg.SILVER_SCHEMA, table=table_config_details['target_table'])
        silver_exists = spark.catalog.tableExists(silver_fqn)
        version_before = (
            spark.sql(f"DESCRIBE HISTORY {silver_fqn}").select("version").first()[0]
            if silver_exists else None
        )


        # ------: Step 3 : Read Bronze partition for run_dt
        bronze_fqn = cfg.fqn(schema=cfg.BRONZE_SCHEMA, table=table_config_details['target_table'])
        bronze_df = spark.table(bronze_fqn).filter(f"dt = '{run_dt}'")
        if bronze_df.isEmpty():
            logger.info(f"[process_one_table_silver] Bronze ingestion is empty."
                         "Skipping Table : {table_name}")

            return {
                    'log_id': process_log_id,
                    'status': cfg.LoadStatus.SKIPPED.value,
                    'table_name': table_config_details['target_table'],
                    'rows_read': 0,
                    'rows_written': 0,
                    'version_before': version_before,
                    'version_after': version_before,
                    'error': None
                }
            

        # ------: Step 4 : Schema enforcement + DQ split
        df_valid, df_quarantine, drift_events = enforce_schema(table_name = table_name,
                                                                df = bronze_df,
                                                                config_rows = config_columns_details,
                                                                drift_policy = drift_policy
                                                                )
        # print(f" >>>--------> Rows in Valid Bronze DF - {df_valid.count()}")
        # print(f" >>>--------> Drift Event Details - {drift_events}")

        # ------: Step 5 : Log Schema drift events
        write_schema_drift_log(run_id = process_run_id,
                                table_id = table_config_details['table_id'],
                                drift_events = drift_events,
                                created_by = cfg.DEFAULT_TRIGGERED_BY
                                )

        # ------: Step 6 : Quarantine bad rows
        write_to_quarantine(df_quarantine = df_quarantine,
                                source_table_name = table_config_details['target_table'],
                                run_id = process_run_id,
                                log_id = process_log_id,
                                run_dt = run_dt,
                                drift_events = drift_events)

        # ------: Step 7 : Dynamic transform dispatch (table-specific logic)
        # module = importlib.import_module(transform_module)
        # df_valid = module.transform(df_valid)   # e.g. AccountID parsing, anything not a plain SQL expr

        # ------: Step 8 : Dedup for merge safety
        df_deduped = dedup_for_merge(df_valid,
                                    business_keys = silver_business_purpose_groups,
                                    order_by_col = 'LastUpdatedAt',
                                    hash_cols = silver_hash_purpose_groups
                                    )
        # print(f" >>>--------> Rows in Silver DF to Process after Dedupe - {df_deduped.count()}")

        # ------: Step 9 : Write strategy dispatch
        write_strategy = spark.sql(f"""
                SELECT 
                    t_process_cfg.write_strategy
                FROM {TABLE_CONFIG} t_cfg
                INNER JOIN {TABLE_PROCESS_CONFIG} t_process_cfg
                ON t_cfg.table_id = t_process_cfg.table_id
                WHERE t_process_cfg.is_active = TRUE
                AND t_process_cfg.process_type = :process_type
                AND t_cfg.source_table_name = :table_name"""
                ,args={"process_type": cfg.ProcessType.SILVER_BUILD.value,
                        "table_name": table_name}
                ).first()[0]
        # process_config_details = get_process_config(table_name=table_name, process_type='SILVER')
        # write_strategy = process_config_details['write_strategy']

        # print(f" >>>--------> Write Strategy for {table_name} - {write_strategy}")
        
        if write_strategy == cfg.WriteStrategy.SCD2.value:
            merge_result = scd2_merge(df_deduped, silver_fqn, silver_business_purpose_groups, run_dt)
        elif write_strategy == cfg.WriteStrategy.UPSERT.value:
            merge_result = upsert_merge(df_deduped, silver_fqn, silver_business_purpose_groups)
        elif write_strategy == cfg.WriteStrategy.APPEND_MERGE.value:
            merge_result = insert_only_merge(df_deduped, silver_fqn, silver_business_purpose_groups)
        else:
            raise ValueError(
                f"[process_one_table_silver] Unknown write_strategy '{write_strategy}' for {table_name}"
            )

        rows_written = merge_result['rows_inserted'] + merge_result['rows_updated']

        # ------: Step 10 : Capture Silver version_after + rows_written
        silver_fqn = cfg.fqn(schema=cfg.SILVER_SCHEMA, table=table_config_details['target_table'])
        silver_exists = spark.catalog.tableExists(silver_fqn)
        version_after = (
            spark.sql(f"DESCRIBE HISTORY {silver_fqn}").select("version").first()[0]
            if silver_exists else None
        )

        # ------: Step 11 : Return status dict
        logger.info(
                    f"[process_one_table_silver] "
                    + f"\n{" " * 15} >>> log_id={process_log_id} "
                    + f"\n{" " * 15} >>> table_name={run_dt} "
                    + f"\n{" " * 15} >>> status={cfg.LoadStatus.SUCCESS.value} "
                    + f"\n{" " * 15} >>> table_name={table_config_details['target_table']} "
                    # + f"\n{" " * 15} >>> rows_read={rows_read} "
                    + f"\n{" " * 15} >>> rows_written={rows_written}"
                    + f"\n{" " * 15} >>> version_before={version_before} "
                    + f"\n{" " * 15} >>> version_after={version_after} "
                    + f"\n{" " * 15} >>> error=None"
                    + "\n" + "-" * 80
                    )

        return {
            'log_id': process_log_id,
            'status': cfg.LoadStatus.SUCCESS.value,
            'table_name': table_config_details['target_table'],
            'rows_read': None,
            'rows_written': None,
            'version_before': version_before,
            'version_after': version_after,
            'error': None
        }

    except Exception as e:
        logger.error(f"[process_one_table_silver] FAILED | error = {e}")
        return {
            'log_id': process_log_id,
            'status': cfg.LoadStatus.FAILED.value,
            'table_name': table_config_details['target_table'],
            'rows_read': None,
            'rows_written': None,
            'version_before': version_before,
            'version_after': version_before,
            'error':  str(e)
        }


# ----====[HELPER FUNC 2 : run_silver_pipeline]=====-----------------------------------------------
def run_silver_pipeline(spark, run_dt: str, trigger_type: str, triggered_by: str, load_type: str, tables: list = None):
    """
    Orchestrates Silver-layer processing for all (or specified) active
    tables for a given run_dt. Wraps start_etl_run / log_table_start /
    process_one_table_silver / log_table_end / end_etl_run per table,
    mirroring the Bronze orchestrator pattern.

    Returns:
        dict with keys:
            run_id, status, total_tables,
            success_count, failed_count, skipped_count,
            error
    """

    # ------: Section 1 : MANUAL/BACKFILL Particular Tables
    try:
        if tables is not None:

            # ------:[Section 1.1 : MANUAL/BACKFILL] Start process : Alyaws outside the loop
            current_run_id = start_etl_run(
                pipeline_name   =   'BFSI_LakeHouse_Pipeline_' + trigger_type,
                process_type    =   cfg.ProcessType.SILVER_BUILD.value,
                run_dt          =   run_dt,
                trigger_type    =   trigger_type,   # 'SCHEDULED' | 'MANUAL' | 'BACKFILL'
                triggered_by    =   triggered_by
            )

            for processing_table in tables:

                # ------:[Section 1.2 : MANUAL/BACKFILL] Process table Logger
                processing_table_id = spark.sql(f"""SELECT table_id FROM {TABLE_CONFIG} WHERE source_table_name = '{processing_table}'""").first()[0]

                table_log_details = log_table_start(
                    run_id               = current_run_id,
                    table_id             = processing_table_id,
                    table_name           = processing_table,
                    process_type         = cfg.ProcessType.SILVER_BUILD.value,
                    load_type            = load_type,
                    run_dt               = run_dt,
                    delta_version_before = None,
                    triggered_by         = cfg.DEFAULT_TRIGGERED_BY
                )

                # ------:[Section 1.3 : MANUAL/BACKFILL] transform table
                process_table_status = process_one_table_silver(    
                                                        spark,
                                                        process_run_id = current_run_id,
                                                        process_log_id = table_log_details['current_log_id'],
                                                        table_name = table_log_details['table_name'],
                                                        run_dt = run_dt
                                                    )

                # ------:[Section 1.4 : MANUAL/BACKFILL] Update Status & Heartbit
                log_table_end(
                    log_id              = process_table_status['log_id'],
                    table_name          = process_table_status['table_name'],
                    status              = process_table_status['status'],                    # 'SUCCESS' | 'FAILED' | 'SKIPPED'
                    rows_read           = process_table_status['rows_read'],
                    rows_written        = process_table_status['rows_written'],
                    delta_version_before = process_table_status['version_before'],
                    delta_version_after = process_table_status['version_after'],
                    error_message       = process_table_status['error'],
                    error_stacktrace    = None
                )
                # ------:[Section 1 : MANUAL/BACKFILL] End process : Loop Ends here.

            # ------:[Section 1.5 : MANUAL/BACKFILL] End Pipeline : Alyaws outside loop for multiple tables.
            end_etl_run_status = end_etl_run(
                                            run_id        = current_run_id,
                                            error_message = process_table_status['error']
                                            )

            return {
                    'run_id': end_etl_run_status['run_id'],
                    'status': end_etl_run_status['status'],
                    'total_tables': end_etl_run_status['total_tables'],
                    'success_count': end_etl_run_status['success_count'],
                    'failed_count': end_etl_run_status['failed_count'],
                    'skipped_count': end_etl_run_status['skipped_count'],
                    'error':  end_etl_run_status['error']
                    }

    except Exception as e:
        logger.error(f"[run_silver_pipeline][Section 1 : MANUAL/BACKFILL] : error - {e}")
        return {
                'run_id': current_run_id,
                'status': 'FAILED',
                'total_tables': 0,
                'success_count': 0,
                'failed_count': 0,
                'skipped_count': 0,
                'error':  str(e)
                }



    try:
        # ------:[Section 2.1 : SCHEDULE RUN] Start process : Alyaws outside the loop
        current_run_id = start_etl_run(
                                    pipeline_name   =   'BFSI_LakeHouse_Pipeline_' + trigger_type,
                                    process_type    =   cfg.ProcessType.SILVER_BUILD.value,
                                    run_dt          =   run_dt,
                                    trigger_type    =   trigger_type,   # 'SCHEDULED' | 'MANUAL' | 'BACKFILL'
                                    triggered_by    =   triggered_by
                                )

        # -----> [Section 2.2 : SCHEDULE RUN] Selection of Active Tables
        list_of_tables = spark.sql(f"""
                SELECT 
                    t_cfg.source_table_name
                FROM {TABLE_CONFIG} t_cfg
                INNER JOIN {TABLE_PROCESS_CONFIG} t_process_cfg
                ON t_cfg.table_id = t_process_cfg.table_id
                WHERE t_process_cfg.is_active = TRUE
                AND t_process_cfg.process_type = :process_type
                ORDER BY t_cfg.load_priority ASC"""
                ,args={"process_type": cfg.ProcessType.SILVER_BUILD.value}
                )

        tables_to_process = [
                            row['source_table_name'] 
                            for row in list_of_tables.collect()
                            ]


        for processing_table in tables_to_process: 

            # ------:[Section 2.2 : SCHEDULE RUN] Process table Logger
            processing_table_id = spark.sql(f"""SELECT table_id FROM bfsi_lakehouse.metadata.table_config WHERE source_table_name = '{processing_table}'""").first()[0]

            table_log_details = log_table_start(
                run_id               = current_run_id,
                table_id             = processing_table_id,
                table_name           = processing_table,
                process_type         = cfg.ProcessType.SILVER_BUILD.value,
                load_type            = load_type,
                run_dt               = run_dt,
                delta_version_before = None,
                triggered_by         = cfg.DEFAULT_TRIGGERED_BY
            )

            # ------:[Section 2.3 : SCHEDULE RUN] transform table
            process_table_status = process_one_table_silver(
                                        spark,
                                        process_run_id = current_run_id,
                                        process_log_id = table_log_details['current_log_id'],
                                        table_name = table_log_details['table_name'],
                                        run_dt = run_dt
                                    )

            # ------:[Section 2.4 : SCHEDULE RUN] Update Status & Heartbit
            log_table_end(
                log_id              = process_table_status['log_id'],
                table_name          = process_table_status['table_name'],
                status              = process_table_status['status'],                    # 'SUCCESS' | 'FAILED' | 'SKIPPED'
                rows_read           = process_table_status['rows_read'],
                rows_written        = process_table_status['rows_written'],
                delta_version_before = process_table_status['version_before'],
                delta_version_after = process_table_status['version_after'],
                error_message       = process_table_status['error'],
                error_stacktrace    = None
            )
            # ------:[Section 2 : SCHEDULE RUN] End process : Loop Ends here.


        # ------:[Section 2.5 : SCHEDULE RUN] End Pipeline : Alyaws outside loop for multiple tables.
        end_etl_run_status = end_etl_run(
                                        run_id        = current_run_id,
                                        error_message = process_table_status['error']
                                        )

        return {
                'run_id': end_etl_run_status['run_id'],
                'status': end_etl_run_status['status'],
                'total_tables': end_etl_run_status['total_tables'],
                'success_count': end_etl_run_status['success_count'],
                'failed_count': end_etl_run_status['failed_count'],
                'skipped_count': end_etl_run_status['skipped_count'],
                'error':  end_etl_run_status['error']
                }

    except Exception as e:
        logger.error(f"[run_silver_pipeline][Section 2 : SCHEDULE RUN] : error - {e}")
        return {
                'run_id': current_run_id,
                'status': 'FAILED',
                'total_tables': 0,
                'success_count': 0,
                'failed_count': 0,
                'skipped_count': 0,
                'error':  str(e)
                }


# ----====[HELPER FUNC 3 : main]=====-----------------------------------------------
if __name__ == "__main__":
    run_dt = '2024-01-02'   # This run_dt will be calculate during orchestration.
    run_silver_pipeline(
                        spark, 
                        run_dt,
                        # trigger_type=cfg.TriggerType.MANUAL.value,    #  MANUAL
                        trigger_type=cfg.TriggerType.SCHEDULED.value,   # SCHEDULED
                        # trigger_type=cfg.TriggerType.BACKFILL.value,    # BACKFILL
                        triggered_by= cfg.DEFAULT_TRIGGERED_BY,         # 'manual_notebook'
                        load_type=cfg.LoadType.FULL.value,
                        # tables=['t_AccountTrx']
                        tables = None
                        )
    
    

    