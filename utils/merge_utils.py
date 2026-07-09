import sys
PROJECT_ROOT = "/Workspace/Users/rohan.m.mukherjee@gmail.com/bfsi-lakehouse-databricks"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
import config.settings as cfg
import uuid
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("No active SparkSession.")

import logging
import sys
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ----====[HELPER : pull MERGE metrics from Delta history — same pattern as Bronze]=====-----------------
def _get_merge_metrics(target_fqn: str) -> dict:
    """
    Read how many rows a MERGE just inserted/updated/deleted — straight from
    Delta's own history, no row counting needed.

    After any MERGE, Delta records what it did in the table's history log.
    This reads the latest history entry and pulls those numbers back.
    Cheap (reads metadata only, never scans the table).

    Args:
        target_fqn : the Delta table just merged into, e.g. 'bfsi_lakehouse.silver.t_client'

    Returns:
        {'rows_inserted': int, 'rows_updated': int, 'rows_deleted': int}
    """
    latest = spark.sql(f"DESCRIBE HISTORY {target_fqn}").select("operationMetrics").first()
    m = latest["operationMetrics"]
    return {
        "rows_inserted": int(m.get("numTargetRowsInserted", 0)),
        "rows_updated":  int(m.get("numTargetRowsUpdated", 0)),
        "rows_deleted":  int(m.get("numTargetRowsDeleted", 0)),
    }


# ----====[STRATEGY 1 : SCD2 MERGE — for t_Client, t_Loan]=====-------------------------------------------
def scd2_merge(df_deduped, silver_fqn: str, business_keys: list, run_dt: str) -> dict:
    try:
        join_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in business_keys])
        key_expr_src = "concat_ws('||', " + ", ".join([f"c.{k}" for k in business_keys]) + ")"
        key_expr_tgt = "concat_ws('||', " + ", ".join([f"tgt.{k}" for k in business_keys]) + ")"

        # ------: Step 1 — First-run bootstrap
        if not spark.catalog.tableExists(silver_fqn):
            (
                df_deduped
                .withColumn("effective_start_dt", F.lit(run_dt).cast("date"))
                .withColumn("effective_end_dt", F.lit(None).cast("date"))
                .withColumn("is_current", F.lit(True))
                .write.format("delta").mode("append").saveAsTable(silver_fqn)
            )
            rows = df_deduped.count()
            logger.info(f"[scd2_merge] First run | {silver_fqn} | bootstrap rows={rows}")
            return {"status": "SUCCESS", "rows_inserted": rows, "rows_updated": 0, "rows_deleted": 0}

        # ------: Step 2 — Identify NEW or CHANGED rows only (unchanged rows excluded — _row_hash check)
        src_view = f"src_{uuid.uuid4().hex[:8]}"
        df_deduped.createOrReplaceTempView(src_view)

        changed_view = f"changed_{uuid.uuid4().hex[:8]}"
        spark.sql(f"""
            SELECT src.*
            FROM {src_view} src
            LEFT JOIN {silver_fqn} tgt
                ON {join_cond} AND tgt.is_current = true
            WHERE tgt.{business_keys[0]} IS NULL
               OR tgt._row_hash != src._row_hash
        """).createOrReplaceTempView(changed_view)

        # ------: Step 3 — Stage two branches: insert-row (NULL key) + close-row (real key)
        all_cols  = df_deduped.columns
        col_list  = ", ".join(all_cols)

        staged_view = f"staged_{uuid.uuid4().hex[:8]}"
        spark.sql(f"""
            SELECT NULL AS _merge_key, {col_list} FROM {changed_view}
            UNION ALL
            SELECT {key_expr_src} AS _merge_key, {col_list}
            FROM {changed_view} c
            WHERE EXISTS (
                SELECT 1 FROM {silver_fqn} tgt
                WHERE {" AND ".join([f"tgt.{k} = c.{k}" for k in business_keys])}
                  AND tgt.is_current = true
            )
        """).createOrReplaceTempView(staged_view)

        # ------: Step 4 — The MERGE
        insert_cols = col_list + ", effective_start_dt, effective_end_dt, is_current"
        insert_vals = ", ".join([f"s.{c}" for c in all_cols]) + ", :run_dt, NULL, true"

        spark.sql(f"""
            MERGE INTO {silver_fqn} AS tgt
            USING {staged_view} AS s
            ON {key_expr_tgt} = s._merge_key AND tgt.is_current = true
            WHEN MATCHED THEN
                UPDATE SET tgt.is_current = false,
                           tgt.effective_end_dt = date_sub(:run_dt, 1)
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """, args={"run_dt": run_dt})

        metrics = _get_merge_metrics(silver_fqn)
        logger.info(f"[scd2_merge] {silver_fqn} | keys={business_keys} | run_dt={run_dt} | {metrics}")
        return {"status": "SUCCESS", **metrics}

    except Exception as e:
        logger.error(f"[scd2_merge] FAILED | {silver_fqn} | error={str(e).split(chr(10))[0][:300]}")
        raise


# ----====[STRATEGY 2 : UPSERT MERGE — for t_LoanInstallment, t_AccountCustomer]=====----------------------
def upsert_merge(df_deduped, silver_fqn: str, business_keys: list) -> dict:
    try:
        if not spark.catalog.tableExists(silver_fqn):
            df_deduped.write.format("delta").mode("append").saveAsTable(silver_fqn)
            return {"status": "SUCCESS", "rows_inserted": df_deduped.count(), "rows_updated": 0, "rows_deleted": 0}

        src_view = f"src_{uuid.uuid4().hex[:8]}"
        df_deduped.createOrReplaceTempView(src_view)

        on_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in business_keys])
        all_cols = df_deduped.columns
        set_clause   = ", ".join([f"tgt.{c} = src.{c}" for c in all_cols if c not in business_keys])
        insert_cols  = ", ".join(all_cols)
        insert_vals  = ", ".join([f"src.{c}" for c in all_cols])

        # ------: NOTE — t_LoanInstallment is 112M rows. Without a pruning predicate, this MERGE
        # scans the whole target to find matches. If your target is partitioned by dt/LoanID range,
        # add that as an extra AND condition in `on_cond` so Delta can data-skip via the transaction log.
        spark.sql(f"""
            MERGE INTO {silver_fqn} AS tgt
            USING {src_view} AS src
            ON {on_cond}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        metrics = _get_merge_metrics(silver_fqn)
        logger.info(f"[upsert_merge] {silver_fqn} | {metrics}")
        return {"status": "SUCCESS", **metrics}

    except Exception as e:
        logger.error(f"[upsert_merge] FAILED | {silver_fqn} | error={str(e).split(chr(10))[0][:300]}")
        raise


# ----====[STRATEGY 3 : INSERT-ONLY MERGE — for t_AccountTrx]=====-----------------------------------------
def insert_only_merge(df_deduped, silver_fqn: str, business_keys: list) -> dict:
    try:
        if not spark.catalog.tableExists(silver_fqn):
            df_deduped.write.format("delta").mode("append").saveAsTable(silver_fqn)
            return {"status": "SUCCESS", "rows_inserted": df_deduped.count(), "rows_updated": 0, "rows_deleted": 0}

        src_view = f"src_{uuid.uuid4().hex[:8]}"
        df_deduped.createOrReplaceTempView(src_view)

        on_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in business_keys])
        all_cols = df_deduped.columns
        insert_cols = ", ".join(all_cols)
        insert_vals = ", ".join([f"src.{c}" for c in all_cols])

        spark.sql(f"""
            MERGE INTO {silver_fqn} AS tgt
            USING {src_view} AS src
            ON {on_cond}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)

        metrics = _get_merge_metrics(silver_fqn)
        logger.info(f"[insert_only_merge] {silver_fqn} | {metrics}")
        return {"status": "SUCCESS", **metrics}

    except Exception as e:
        logger.error(f"[insert_only_merge] FAILED | {silver_fqn} | error={str(e).split(chr(10))[0][:300]}")
        raise