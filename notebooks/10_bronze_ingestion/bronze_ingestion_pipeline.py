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
    end_etl_run
)
from utils.metadata_utils import (
    get_table_config,
    get_process_config
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


# ----====[HELPER FUNC 1 : ingest_one_table]=====-----------------------------------------------
def ingest_one_table(spark, log_id, table_name:str, run_dt) -> dict:
        """
            Ingests a single table's data into the bronze layer based on input parameters.
            Used for ingesting multiple tables one by one in a pipeline.
            Returns : 
                status, rows_read, rows_written,version_before, version_after
        """
        try:
            logger.info(f"[ingest_one_table] Ingestion Process Started for table - {table_name}")


            # ------: Get Config Details
            table_config_data = get_table_config(table_name = table_name)
            # print(f"table_config_data = {table_config_data}")

            table_process_config_data = get_process_config(table_name = table_name, process_type = cfg.ProcessType.BRONZE_INGEST.value)
            # print(f"Table_process_config_data = {table_process_config_data}")


            # ------: Assemble config details for source & destination path
            source_table_name = table_config_data['source_table_name']
            source_path_pattern = table_config_data['source_path_pattern']
            source_format = table_config_data['source_format']

            target_catalog = table_config_data['target_catalog']
            target_schema = table_config_data['target_schema']
            target_table = table_config_data['target_table']

            partition_cols = table_config_data['partition_cols']
            replace_where_template = table_config_data['replace_where_template']

            drift_policy = table_config_data['drift_policy']

            if source_format.lower() == 'parquet':
                actual_source_path = source_path_pattern.replace('{run_dt}',run_dt)
            else:
                raise Exception(f"Source format {source_format} not supported.")

            destination_table = cfg.fqn(schema = cfg.BRONZE_SCHEMA, table = target_table)        
            
            # ------: Read parquet
            df = spark.read.format(source_format).load(actual_source_path)
            rows_read = df.count()

            # ------: Add audit columns
            df = df.withColumn('dt', F.lit(run_dt)) \
                    .withColumn(cfg.AuditColumns.INGESTION_RUN_ID, F.lit(log_id)) \
                    .withColumn(cfg.AuditColumns.INGESTED_AT, F.current_timestamp()) \
                    .withColumn(cfg.AuditColumns.SOURCE_FILE, F.col("_metadata.file_path"))
            
            
            # ------: Capture Delta Version & Check target table exists or not
            table_exists = spark.catalog.tableExists(destination_table)
            if not table_exists:
                version_before = None   # For 1st run senarios
            else:
                version_before = (
                                spark.sql(f"DESCRIBE HISTORY {destination_table}")
                                    .select("version")
                                    .first()[0]
                                )
                

            # ------: If dataframe is empty then pass to next table & mark current status to SKIPPED
            if rows_read == 0:
                logger.info(
                            f"[ingest_one_table] "
                            + f"\n{" " * 15} >>> log_id={run_id} "
                            + f"\n{" " * 15} >>> status={cfg.LoadStatus.SUCCESS.value} "
                            + f"\n{" " * 15} >>> table_name={table_name} "
                            + f"\n{" " * 15} >>> rows_read={rows_read} "
                            + f"\n{" " * 15} >>> rows_written=0 "
                            + f"\n{" " * 15} >>> version_before={version_before} "
                            + f"\n{" " * 15} >>> version_after={version_before} "
                            + f"\n{" " * 15} >>> error=None"
                            + "\n" + "-" * 80
                        )

                return {
                    'log_id': log_id,
                    'status': cfg.LoadStatus.SUCCESS.value,
                    'table_name':table_name,
                    'rows_read': rows_read,
                    'rows_written': 0,
                    'version_before': version_before,
                    'version_after': version_before,
                    'error': None
                }
                
            
            # ------: Write to Bronze Delta
            # print(f"{replace_where_template.replace('{run_dt}',run_dt)}")
            
            # logger.info(f"[DEBUG] table : {table_name} | destination_table_path = {destination_table}")
            # logger.info(f"[DEBUG] table : {table_name} | table_exists = {table_exists}")
                        
            if not table_exists:    # Only for 1st run
                writer = (
                        df.write
                        .format("delta")
                        .partitionBy("dt")
                    )
                writer.saveAsTable(destination_table)
            else:
                writer = (
                            df.write
                            .format("delta")
                            .mode("overwrite")
                            .option("replaceWhere", f"{replace_where_template.replace('{run_dt}',run_dt)}")
                            .partitionBy("dt")
                        )
                writer.saveAsTable(destination_table)
            
            # ------: Capture Delta version after write & rows written
            history = spark.sql(f"DESCRIBE HISTORY {destination_table}")
            latest = (
                        history
                        .select("version", "operation", "operationMetrics")
                        .first()
                    )

            version_after = latest["version"]
            # operation = latest["operation"]
            rows_written = latest["operationMetrics"].get("numOutputRows")

            # ------: Return final status after write
            logger.info(
                        f"[ingest_one_table] "
                        + f"\n{" " * 15} >>> log_id={log_id} "
                        + f"\n{" " * 15} >>> status={cfg.LoadStatus.SUCCESS.value} "
                        + f"\n{" " * 15} >>> table_name={table_name} "
                        + f"\n{" " * 15} >>> rows_read={rows_read} "
                        + f"\n{" " * 15} >>> rows_written={rows_written}"
                        + f"\n{" " * 15} >>> version_before={version_before} "
                        + f"\n{" " * 15} >>> version_after={version_after} "
                        + f"\n{" " * 15} >>> error=None"
                        + "\n" + "-" * 80
                        )
            return {
                    'log_id': log_id,
                    'status': cfg.LoadStatus.SUCCESS.value, 
                    'table_name':table_name,
                    'rows_read': rows_read, 
                    'rows_written': rows_written,
                    'version_before':version_before, 
                    'version_after': version_after,
                    'error':None
                    }
        
        except Exception as e:
            logger.error(f"[ingest_one_table] 'status': {cfg.LoadStatus.FAILED.value},"
                             f"'table':{table_name},"
                             f"'run_id':{run_id},"
                             f"'error': {e}")
            return {
                        'status': cfg.LoadStatus.FAILED.value,
                        'table_name':table_name,
                        'rows_read': 0,
                        'rows_written': 0,
                        'version_before': None,
                        'version_after': None,
                        'error': str(e)
                    }
        



# ----====[HELPER FUNC 2 : run_bronze_pipeline]=====-----------------------------------------------
def run_bronze_pipeline(spark, run_dt:str,trigger_type:str,triggered_by:str,load_type:str, tables:list=None):
    """
        # Orchestrates the ingestion of all tables in the bronze layer.
        # Returns : 
        #     status, rows_read, rows_written, version_after
    """

    # ------: Section 1 : MANUAL/BACKFILL Particular Tables
    try:
        if tables is not None:

            # ------:[Section 1.1 : MANUAL/BACKFILL] Start process : Alyaws outside the loop
            current_run_id = start_etl_run(
                pipeline_name   =   'BFSI_LakeHouse_Pipeline_' + trigger_type,
                process_type    =   cfg.ProcessType.BRONZE_INGEST.value,
                run_dt          =   run_dt,
                trigger_type    =   trigger_type,   # 'SCHEDULED' | 'MANUAL' | 'BACKFILL'
                triggered_by    =   triggered_by
            )

            for processing_table in tables:
                # print(f"processing_table : {processing_table}")

                # ------:[Section 1.2 : MANUAL/BACKFILL] Process table Logger
                processing_table_id = spark.sql(f"""SELECT table_id FROM {TABLE_CONFIG} WHERE source_table_name = '{processing_table}'""").first()[0]

                table_log_details = log_table_start(
                    run_id               = current_run_id,
                    table_id             = processing_table_id,
                    table_name           = processing_table,
                    process_type         = cfg.ProcessType.BRONZE_INGEST.value,
                    load_type            = load_type,
                    run_dt               = run_dt,
                    delta_version_before = None,
                    triggered_by         = cfg.DEFAULT_TRIGGERED_BY
                )
                # current_log_id = table_log_details['current_log_id']

                # ------:[Section 1.3 : MANUAL/BACKFILL] ingest table
                ingest_table_status = ingest_one_table(
                                                        spark,
                                                        log_id = table_log_details['current_log_id'],
                                                        table_name = table_log_details['table_name'],
                                                        run_dt = run_dt
                                                    )


                # ------:[Section 1.4 : MANUAL/BACKFILL] Update Status & Heartbit
                log_table_end(
                    log_id              = ingest_table_status['log_id'],
                    table_name          = ingest_table_status['table_name'],
                    status              = ingest_table_status['status'],                    # 'SUCCESS' | 'FAILED' | 'SKIPPED'
                    rows_read           = ingest_table_status['rows_read'],
                    rows_written        = ingest_table_status['rows_written'],
                    delta_version_before = ingest_table_status['version_before'],
                    delta_version_after = ingest_table_status['version_after'],
                    error_message       = ingest_table_status['error'],
                    error_stacktrace    = None
                )
                # ------:[Section 1 : MANUAL/BACKFILL] End process : Loop Ends here.


            # ------:[Section 1.5 : MANUAL/BACKFILL] End Pipeline : Alyaws outside loop for multiple tables.
            end_etl_run_status = end_etl_run(
                                            run_id        = current_run_id,
                                            error_message = ingest_table_status['error']
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
        logger.error(f"[run_bronze_pipeline][Section 1 : MANUAL/BACKFILL] : error - {e}")
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
                                    process_type    =   cfg.ProcessType.BRONZE_INGEST.value,
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
                AND t_process_cfg.process_type = :process_type"""
                ,args={"process_type": cfg.ProcessType.BRONZE_INGEST.value}
                )
        
        tables_to_process = [
                            row['source_table_name'] 
                            for row in list_of_tables.collect()
                            ]


        for processing_table in tables_to_process: 
            # print(f"processing_table : {processing_table}")

            # ------:[Section 2.2 : SCHEDULE RUN] Process table Logger
            processing_table_id = spark.sql(f"""SELECT table_id FROM bfsi_lakehouse.metadata.table_config WHERE source_table_name = '{processing_table}'""").first()[0]

            table_log_details = log_table_start(
                run_id               = current_run_id,
                table_id             = processing_table_id,
                table_name           = processing_table,
                process_type         = cfg.ProcessType.BRONZE_INGEST.value,
                load_type            = load_type,
                run_dt               = run_dt,
                delta_version_before = None,
                triggered_by         = cfg.DEFAULT_TRIGGERED_BY
            )

            # ------:[Section 2.3 : SCHEDULE RUN] ingest table
            ingest_table_status = ingest_one_table(
                                                    spark,
                                                    log_id = table_log_details['current_log_id'],
                                                    table_name = table_log_details['table_name'],
                                                    run_dt = run_dt
                                                )


            # ------:[Section 2.4 : SCHEDULE RUN] Update Status & Heartbit
            log_table_end(
                log_id              = ingest_table_status['log_id'],
                table_name          = ingest_table_status['table_name'],
                status              = ingest_table_status['status'],                    # 'SUCCESS' | 'FAILED' | 'SKIPPED'
                rows_read           = ingest_table_status['rows_read'],
                rows_written        = ingest_table_status['rows_written'],
                delta_version_before = ingest_table_status['version_before'],
                delta_version_after = ingest_table_status['version_after'],
                error_message       = ingest_table_status['error'],
                error_stacktrace    = None
            )
            # ------:[Section 2 : SCHEDULE RUN] End process : Loop Ends here.


        # ------:[Section 2.5 : SCHEDULE RUN] End Pipeline : Alyaws outside loop for multiple tables.
        end_etl_run_status = end_etl_run(
                                        run_id        = current_run_id,
                                        error_message = ingest_table_status['error']
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
        logger.error(f"[run_bronze_pipeline][Section 2 : SCHEDULE RUN] : error - {e}")
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
    run_bronze_pipeline(
                        spark, 
                        run_dt,
                        # trigger_type=cfg.TriggerType.MANUAL.value,    #  MANUAL
                        trigger_type=cfg.TriggerType.SCHEDULED.value,   # SCHEDULED
                        # trigger_type=cfg.TriggerType.BACKFILL.value,    # BACKFILL
                        triggered_by= cfg.DEFAULT_TRIGGERED_BY,         # 'manual_notebook'
                        load_type=cfg.LoadType.FULL.value,
                        tables=None                                     # ['t_Client','t_AccountCustomer'] # 
                        )

    # ingest_one_table(spark, run_id = '123', table_name = 't_Client', run_dt = '2024-01-02')
    
    

