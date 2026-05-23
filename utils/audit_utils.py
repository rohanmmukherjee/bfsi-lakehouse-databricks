import sys
PROJECT_ROOT = "/Workspace/Users/rohan.m.mukherjee@gmail.com/bfsi-lakehouse-databricks"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
print(f"✓ Project root added to sys.path: {PROJECT_ROOT}")



import config.settings as cfg
import uuid
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


# ----====[MASTER TABLES CONFIG]=====------------------------------------------------------------------------------------------------
MASTER_TABLE = cfg.TBL_ETL_PROCESS_MASTER
LOG_TABLE    = cfg.TBL_ETL_PROCESS_LOG


# ----====[LOGGIN HELPER FUNC 1 : To insert a fresh log row whenever ETL starts.]=====-----------------------------------------------
def start_etl_run(
    pipeline_name: str,
    process_type: str,              # 'BRONZE' | 'SILVER' | 'GOLD'
    run_dt: str,                    # 'YYYY-MM-DD' (string, same as schema)
    trigger_type: str = "MANUAL",   # 'SCHEDULED' | 'MANUAL' | 'BACKFILL'
    triggered_by: str = "manual"
) -> str:
    """
    Insert new row into etl_process_master with status='STARTED'.
    Returns: run_id (UUID string)
    """
    try:
        current_run_id = str(uuid.uuid4())
        
        spark.sql(f"""
                INSERT INTO {MASTER_TABLE}
                (run_id,pipeline_name,process_type,run_dt,trigger_type,triggered_by,status,started_at,created_by,created_at,updated_at)
                VALUES (:run_id,:pipeline_name,:process_type,:run_dt,:trigger_type,:triggered_by,'STARTED',
                current_timestamp(),:created_by,current_timestamp(),current_timestamp())
                """, args={"run_id": current_run_id,
                            "pipeline_name":pipeline_name,
                            "process_type":process_type,
                            "run_dt":str(run_dt),
                            "trigger_type":trigger_type,
                            "triggered_by":triggered_by,
                            "created_by":triggered_by
                        })
        
        logger.info(f"[start_etl_run] run_id={current_run_id} | pipeline={pipeline_name} | prcoess_type={process_type} | dt={run_dt} | status=STARTED")
        return current_run_id
    
    except Exception as e:
        logger.error(f"[start_etl_run] FAILED | run_id={current_run_id} | error={e}")
        raise


# ----====[LOGGIN HELPER FUNC 2 : While ETL process is running, log the initial status of each table.]=====------------------------------------
def log_table_start(
    run_id: str,
    table_id: int,
    table_name: str,
    process_type: str,              # 'BRONZE' | 'SILVER' | 'GOLD'
    load_type: str,                 # 'FULL' | 'INCREMENTAL'
    run_dt: str,
    delta_version_before: int | None = None,
    triggered_by = "manual"
) -> str:
    """
    Insert new row into etl_process_log with status='STARTED'.
    Returns: log_id (UUID string)
    """
    try:
        current_log_id = str(uuid.uuid4())
        
        spark.sql(f"""
                INSERT INTO {LOG_TABLE}
                (log_id,run_id,table_id,table_name,process_type,load_type,run_dt,status,delta_version_before,started_at,heartbeat_at,created_by,created_at,updated_at)
                VALUES (:log_id,:run_id,:table_id,:table_name,:process_type,:load_type,:run_dt,'STARTED',:delta_version_before,
                current_timestamp(),current_timestamp(),:created_by,current_timestamp(),current_timestamp())
                """, args={
                            "log_id": current_log_id,
                            "run_id": run_id,
                            "table_id":table_id,
                            "table_name":table_name,
                            "process_type":process_type,
                            "load_type":load_type,
                            "run_dt":str(run_dt),
                            "delta_version_before":delta_version_before,
                            "created_by":triggered_by
                        })
        
        logger.info(f"[log_table_start] log_id={current_log_id} | table_name={table_name} | status=STARTED")
        return current_log_id
    except Exception as e:
        logger.error(f"[log_table_start] FAILED | log_id={current_log_id} | error={e}")
        raise
    

# ----====[LOGGIN HELPER FUNC 3 : Update log status for each table after every operation]=====-----------------------------------------------
def log_table_end(
    log_id: str,
    status: str,                    # 'SUCCESS' | 'FAILED' | 'SKIPPED'
    rows_read: int | None = None,
    rows_written: int | None = None,
    delta_version_after: int | None = None,
    error_message: str | None = None,
    error_stacktrace: str | None = None
) -> None:
    """
    UPDATE existing etl_process_log row by log_id.
    Sets finished_at = current_timestamp().
    """
    try:        
        spark.sql(f"""
                UPDATE {LOG_TABLE}
                SET status = :status,
                    rows_read = :rows_read,
                    rows_written = :rows_written,
                    delta_version_after = :delta_version_after,
                    finished_at = current_timestamp(),
                    error_message = :error_message,
                    error_stacktrace = :error_stacktrace,
                    updated_at = current_timestamp(),
                    heartbeat_at = current_timestamp(),
                    duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(started_at)
                    WHERE log_id = :log_id
                """, args={
                            "log_id": log_id,
                            "status": status,
                            "rows_read": rows_read,
                            "rows_written": rows_written,
                            "delta_version_after": delta_version_after,
                            "error_message": error_message,
                            "error_stacktrace":error_stacktrace
                        })
        
        logger.info(f"[log_table_end] log_id={log_id} | status={status} | rows_read={rows_read} | rows_written={rows_written}")
        return None
    except Exception as e:
        logger.error(f"[log_table_end] FAILED | log_id={log_id} | error={e}")
        raise


# ----====[LOGGIN HELPER FUNC 4 : Update ETL final process status.]=====-----------------------------------------------
def end_etl_run(
    run_id: str,
    error_message: str | None = None
    # ,status: str                     # 'SUCCESS' | 'FAILED' | 'PARTIAL'
) -> None:
    """
    UPDATE etl_process_master row.
    Computes total_tables, success_count, failed_count, skipped_count
    by aggregating from etl_process_log WHERE run_id = ?.
    Sets finished_at, duration_seconds.
    """
    try:
        rows = spark.sql(f"""
                                SELECT run_id,
                                        COUNT(*) AS total_tables,
                                        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
                                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
                                        SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped_count
                                FROM {LOG_TABLE}
                                WHERE run_id = :run_id
                                GROUP BY run_id"""
                                , args={"run_id": run_id}).collect()
        
        if not rows:
            raise ValueError(f"[end_etl_run] No log entries found for run_id={run_id}."
                            f"Ensure log_table_start was called before end_etl_run.")

        log_status = rows[0]

        success_count = log_status['success_count']
        failed_count = log_status['failed_count']
        skipped_count = log_status['skipped_count']
        total_tables = log_status['total_tables']

        if failed_count == 0:
            status = 'SUCCESS'
        elif failed_count == total_tables:
            status = 'FAILED'
        elif failed_count > 0:
            status = 'PARTIAL'
        else:
            status = 'UNKNOWN'

        spark.sql(f"""
                  UPDATE {MASTER_TABLE}
                  SET status = :status,
                        total_tables = :total_tables,
                        success_count = :success_count,
                        failed_count = :failed_count,
                        skipped_count = :skipped_count,
                        finished_at = current_timestamp(),
                        duration_seconds = unix_timestamp(current_timestamp()) - unix_timestamp(started_at),
                        updated_at = current_timestamp(),
                        error_message = :error_message
                    WHERE run_id = :run_id
                  """,args={
                            "run_id": run_id,
                            "status": status,
                            "total_tables": total_tables,
                            "success_count": success_count,
                            "failed_count": failed_count,
                            "skipped_count": skipped_count,
                            "error_message": error_message
                        })
        
        logger.info(f"[end_etl_run] run_id={run_id} | status={status} | total_tables={total_tables} | success_count={success_count} | failed_count={failed_count} | skipped_count={skipped_count}")
        return None
    except Exception as e:
        logger.error(f"[end_etl_run] FAILED | run_id={run_id} | error={e}")
        raise


# ----====[LOGGIN HELPER FUNC 5 : Log Schema drift changes]=====-----------------------------------------------
# write_schema_drift_log()
     