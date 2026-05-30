"""
config.py
---------
Central static configuration for the BFSI Lakehouse framework.

Contains ONLY values that:
  - Do not change between runs of the same environment
  - Are referenced by 2+ utilities/notebooks
  - Need a single source of truth (changing here = changing everywhere)

Does NOT contain:
  - Per-table behaviour  →  read from metadata.table_config at runtime
  - Watermarks / run state  →  derived from metadata.etl_process_log
  - Schema definitions  →  built from metadata.input_column_config

"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Final


# ============================================================
# 1. ENVIRONMENT
# ============================================================
class Environment(str, Enum):
    DATABRICKS_SERVERLESS = "databricks_serverless"
    DATABRICKS_CLASSIC    = "databricks_classic"
    LOCAL                 = "local"


def get_env() -> Environment:
    """
    Detect runtime environment. Order of precedence:
      1. Explicit env var BFSI_ENV (override for testing)
      2. Databricks runtime markers
      3. Default to LOCAL
    """
    override = os.environ.get("BFSI_ENV")
    if override:
        return Environment(override)

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Serverless sets IS_SERVERLESS=TRUE; classic does not
        if os.environ.get("IS_SERVERLESS", "").upper() == "TRUE":
            return Environment.DATABRICKS_SERVERLESS
        return Environment.DATABRICKS_CLASSIC

    return Environment.LOCAL


# ============================================================
# 2. UNITY CATALOG NAMESPACE
# ============================================================
CATALOG: Final[str] = "bfsi_lakehouse"

METADATA_SCHEMA: Final[str] = "metadata"
RAW_SCHEMA:      Final[str] = "raw"
BRONZE_SCHEMA:   Final[str] = "bronze"
SILVER_SCHEMA:   Final[str] = "silver"
GOLD_SCHEMA:     Final[str] = "gold"


def fqn(schema: str, table: str) -> str:
    """Build a fully-qualified Unity Catalog name: catalog.schema.table"""
    return f"{CATALOG}.{schema}.{table}"


# ============================================================
# 3. METADATA TABLE FQNs
# ============================================================
TBL_TABLE_CONFIG:         Final[str] = fqn(METADATA_SCHEMA, "table_config")
TBL_TABLE_PROCESS_CONFIG: Final[str] = fqn(METADATA_SCHEMA, "table_process_config")
TBL_INPUT_COLUMN_CONFIG:  Final[str] = fqn(METADATA_SCHEMA, "input_column_config")
TBL_ETL_PROCESS_MASTER:   Final[str] = fqn(METADATA_SCHEMA, "etl_process_master")
TBL_ETL_PROCESS_LOG:      Final[str] = fqn(METADATA_SCHEMA, "etl_process_log")
TBL_SCHEMA_DRIFT_LOG:     Final[str] = fqn(METADATA_SCHEMA, "schema_drift_log")


# ============================================================
# 4. RAW LANDING ZONE
# ----  Volume path. Templated with {dt} and {batch}.
#       table_config.source_path_pattern is the authoritative source;
#       this is the FALLBACK / default pattern only.
# ============================================================
RAW_VOLUME_ROOT: Final[str] = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/synthetic_data"
RAW_PATH_TEMPLATE: Final[str] = RAW_VOLUME_ROOT + "/{source_table}/dt={dt}/batch={batch}"


# ============================================================
# 5. BRONZE AUDIT COLUMNS
# ----  Names stamped on every Bronze row by delta_writer.
#       Centralized so notebooks never hardcode these strings.
# ============================================================
@dataclass(frozen=True)
class AuditColumns:
    INGESTION_RUN_ID: str = "_ingestion_run_id"
    INGESTED_AT:      str = "_ingested_at"
    SOURCE_FILE:      str = "_source_file"      # input_file_name() — optional but useful

    def as_list(self) -> list[str]:
        return [self.INGESTION_RUN_ID, self.INGESTED_AT, self.SOURCE_FILE]


AUDIT_COLS: Final[AuditColumns] = AuditColumns()


# ============================================================
# 6. ENUMS — single source of truth for status / policy strings
# ----  CHECK constraints in the metadata DDL must match these.
# ============================================================
class DriftPolicy(str, Enum):
    STRICT     = "STRICT"
    EVOLVE     = "EVOLVE"
    QUARANTINE = "QUARANTINE"


class LoadStatus(str, Enum):
    STARTED   = "STARTED"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"          # source path empty / no new data
    QUARANTINED = "QUARANTINED"    # drift triggered park


class ProcessType(str, Enum):
    BRONZE_INGEST = "BRONZE"
    SILVER_BUILD  = "SILVER"
    GOLD_BUILD    = "GOLD"


class LoadType(str, Enum):
    FULL        = "FULL"
    INCREMENTAL = "INCREMENTAL"


class TriggerType(str, Enum):
    SCHEDULED = "SCHEDULED"
    MANUAL    = "MANUAL"
    BACKFILL  = "BACKFILL"


# ============================================================
# 7. SPARK RUNTIME DEFAULTS
# ----  Used by spark_session.py when env == LOCAL.
#       On Databricks, session is pre-configured — these are ignored.
# ============================================================
@dataclass(frozen=True)
class SparkLocalConfig:
    app_name:          str = "bfsi_lakehouse_local"
    master:            str = "local[4]"
    shuffle_partitions: int = 18
    driver_memory:     str = "8g"
    executor_memory:   str = "8g"
    ansi_enabled:      bool = True            # parity with Databricks Serverless
    extra_conf: dict[str, str] = field(default_factory=lambda: {
        "spark.sql.session.timeZone": "Asia/Kolkata",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    })


SPARK_LOCAL: Final[SparkLocalConfig] = SparkLocalConfig()


# ============================================================
# 8. RUNTIME DEFAULTS / GUARDRAILS
# ============================================================
DEFAULT_TRIGGERED_BY:   Final[str] = "scheduled_notebook"
MAX_BACKFILL_DAYS:      Final[int] = 30        # safety cap on accidental long backfills
DEFAULT_DRIFT_POLICY:   Final[DriftPolicy] = DriftPolicy.STRICT
DEFAULT_LOAD_PRIORITY:  Final[int] = 100

# Delta tuning
BRONZE_TARGET_FILE_SIZE_MB: Final[int] = 128
BRONZE_TBL_PROPERTIES: Final[dict[str, str]] = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact":   "true",
    "delta.columnMapping.mode":         "name",
    "delta.minReaderVersion":           "2",
    "delta.minWriterVersion":           "5",
}


# ============================================================
# 9. LOGGING
# ============================================================
LOG_FORMAT: Final[str] = (
    "%(asctime)s | %(levelname)-7s | %(name)s | run_id=%(run_id)s | %(message)s"
)
LOG_LEVEL_DEFAULT: Final[str] = "INFO"


# ============================================================
# 10. SELF-CHECK (run when module imported directly for debugging)
# ============================================================
# if __name__ == "__main__":
#     print(f"Environment       : {get_env()}")
#     print(f"Catalog           : {CATALOG}")
#     print(f"Metadata FQNs     :")
#     for t in (TBL_TABLE_CONFIG, TBL_TABLE_PROCESS_CONFIG, TBL_INPUT_COLUMN_CONFIG,
#               TBL_ETL_PROCESS_MASTER, TBL_ETL_PROCESS_LOG, TBL_SCHEMA_DRIFT_LOG):
#         print(f"  - {t}")
#     print(f"Raw path template : {RAW_PATH_TEMPLATE}")
#     print(f"Audit columns     : {AUDIT_COLS.as_list()}")
#     print(f"Drift policies    : {[p.value for p in DriftPolicy]}")
#     print(f"Load statuses     : {[s.value for s in LoadStatus]}")