# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic LMS Data Generator (Loan-Only) — Serverless-Clean
# MAGIC
# MAGIC **Output:** Parquet files in `/Volumes/bfsi_lakehouse/raw/synthetic_data/<table>/dt=YYYY-MM-DD/batch=N/`
# MAGIC
# MAGIC **Serverless-specific changes:**
# MAGIC - No `Window.partitionBy().orderBy()` for TrxID — replaced with deterministic hash
# MAGIC - No `.cache()` / `.unpersist()` (Serverless avoids manual caching idioms)
# MAGIC - No `state["last_trx_id"]` tracking — hash-based IDs are stateless

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta
import json
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# --- Paths (Unity Catalog Volume) ---
OUTPUT_PATH = "/Volumes/bfsi_lakehouse/raw/synthetic_data/"
STATE_FILE  = f"{OUTPUT_PATH}/_state/last_ids.json"

# --- Date range ---
BACKFILL_START_DATE = "2024-02-01"
BACKFILL_END_DATE   = "2024-02-29"

# --- Scale (S tier) ---
MIN_LOANS_PER_DAY = 50_000
MAX_LOANS_PER_DAY = 60_000
REPAYMENT_RATE    = 0.8       # 80% of due installments repay; 20% skip → DPD

# --- DQ injection rates (always on, fixed seeds for determinism) ---
DQ_NULL_STATE_RATE       = 0.01     # 1.00% of t_Client.State → NULL
DQ_DUP_LOAN_RATE         = 0.005    # 0.50% of t_Loan rows duplicated
DQ_MALFORMED_ACCID_RATE  = 0.001    # 0.10% of t_AccountTrx.AccountID truncated
DQ_FUTURE_CREATEDAT_RATE = 0.0005   # 0.05% of t_AccountTrx.CreatedAt shifted +30d

DQ_SEED_NULL_STATE       = 45
DQ_SEED_DUP_LOAN         = 42
DQ_SEED_MALFORMED_ACCID  = 43
DQ_SEED_FUTURE_CREATEDAT = 44
RAND_SEED_REPAYMENT      = 99

# --- Reference data ---
N_BRANCHES   = 50
FUND_IDS     = [f"FUND{str(i).zfill(3)}" for i in range(1, 11)]
STATES       = ["MH", "DL", "KA", "TN", "GJ", "UP", "WB", "RJ", "MP", "AP"]
USERS        = [f"USER{str(i).zfill(4)}" for i in range(1, 101)]
PAYMENT_MODES = ["NEFT", "RTGS", "IMPS", "CASH", "CHEQUE", "UPI", "DD"]
FIRST_NAMES  = ["Aarav","Priya","Rahul","Sunita","Vikram","Anjali","Ravi",
                "Meena","Suresh","Pooja","Amit","Kavita","Deepak","Nisha"]
LAST_NAMES   = ["Sharma","Patel","Singh","Kumar","Gupta","Joshi","Mehta",
                "Shah","Verma","Yadav","Nair","Reddy","Das","Mishra"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper Functions

# COMMAND ----------

def pick(lst, id_col, alias=None):
    """Deterministic pick from a list using modulo on an id column."""
    n = len(lst)
    expr = F.when(id_col % n == 0, lst[0])
    for i in range(1, n):
        expr = expr.when(id_col % n == i, lst[i])
    expr = expr.otherwise(lst[0])
    return expr.alias(alias) if alias else expr


def branch_id_expr(id_col):
    """Map id → 4-digit BranchID (0001…0050)."""
    return F.lpad(((id_col % N_BRANCHES) + 1).cast(StringType()), 4, "0")


def path_exists(path: str) -> bool:
    """True if a Volumes/DBFS path exists."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def get_next_batch(dt: str) -> int:
    """
    Anchor on t_Client. If no batch exists for this dt, return 1; else max+1.
    All 5 tables write to the SAME batch number per dt for alignment.
    """
    base = f"{OUTPUT_PATH}/t_Client/dt={dt}"
    if not path_exists(base):
        return 1
    entries = dbutils.fs.ls(base)
    batches = [
        int(e.name.rstrip("/").replace("batch=", ""))
        for e in entries if e.name.startswith("batch=")
    ]
    return max(batches) + 1 if batches else 1


def write_batch(df, table_name, partition_suffix, n_partitions=10):
    path = f"{OUTPUT_PATH}/{table_name}/{partition_suffix}"
    print(f"   Writing → .../{table_name}/{partition_suffix}")
    df.repartition(n_partitions).write.mode("overwrite").parquet(path)
    count = spark.read.parquet(path).count()
    print(f"   ✓ {table_name:<22}  {count:>12,} rows  | {len(df.columns)} cols")
    return count


def get_date_range():
    start = date.fromisoformat(BACKFILL_START_DATE) if BACKFILL_START_DATE else date.today()
    end   = date.fromisoformat(BACKFILL_END_DATE)   if BACKFILL_END_DATE   else start
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. State Management
# MAGIC
# MAGIC State now tracks only client/account/loan sequences. TrxID is hash-derived.

# COMMAND ----------

DEFAULT_STATE = {
    "last_client_id": 0,
    "last_acc_seq":   0,
    "last_loan_seq":  0,
    "total_runs":     0,
}

def read_state():
    if not path_exists(STATE_FILE):
        print("   No state file — FIRST RUN.")
        return DEFAULT_STATE.copy()
    content = dbutils.fs.head(STATE_FILE, 65536)
    state = json.loads(content)
    print(f"   State loaded → last_client_id={state['last_client_id']:,}, "
          f"last_loan_seq={state['last_loan_seq']:,}, runs={state['total_runs']}")
    return state

def write_state(state):
    dbutils.fs.put(STATE_FILE, json.dumps(state, indent=2), overwrite=True)
    print(f"   State saved → last_client_id={state['last_client_id']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Main Generation Loop

# COMMAND ----------

state = read_state()
DATE_RANGE = get_date_range()

for run_date_obj in DATE_RANGE:
    run_date_str = run_date_obj.strftime("%Y-%m-%d")
    BATCH_NUM = get_next_batch(run_date_str)
    PARTITION_SUFFIX = f"dt={run_date_str}/batch={BATCH_NUM}"

    print(f"\n>> {run_date_str}  batch={BATCH_NUM}")

    # =========================================================================
    # STEP 1 — Generate clients, accounts, loans, installments
    # =========================================================================
    n_new_loans     = random.randint(MIN_LOANS_PER_DAY, MAX_LOANS_PER_DAY)
    CLIENT_ID_START = state["last_client_id"] + 1
    CLIENT_ID_END   = CLIENT_ID_START + n_new_loans
    ACC_SEQ_START   = state["last_acc_seq"] + 1
    LOAN_SEQ_START  = state["last_loan_seq"] + 1

    print(f"   Generating {n_new_loans:,} loans (clients {CLIENT_ID_START:,}–{CLIENT_ID_END-1:,})")

    # ---- t_Client ----
    client_df = (
        spark.range(CLIENT_ID_START, CLIENT_ID_END)
        .withColumn("BranchID",      branch_id_expr(F.col("id")))
        .withColumn("OurBranchID",   F.col("BranchID"))
        .withColumn("ClientID",      F.concat(F.lit("CL"),
                                              F.lpad(F.col("id").cast(StringType()), 10, "0")))
        .withColumn("ClientName",    F.concat(
                                         pick(FIRST_NAMES, F.col("id")),
                                         F.lit(" "),
                                         pick(LAST_NAMES,  F.col("id") + F.lit(7))))
        .withColumn("ClientType",    F.when(F.col("id") % 10 == 0, "CORPORATE")
                                      .when(F.col("id") % 5  == 0, "SME")
                                      .otherwise("INDIVIDUAL"))
        .withColumn("DOB",           F.date_sub(F.current_date(),
                                         (F.col("id") % (57 * 365) + 18 * 365).cast(IntegerType())))
        .withColumn("IsActive",      F.when(F.col("id") % 20 == 0, F.lit(False)).otherwise(F.lit(True)))
        .withColumn("State",         pick(STATES, F.col("id"), "State"))
        .withColumn("Country",       F.lit("IND"))
        .withColumn("CreatedAt",     F.to_timestamp(F.lit(run_date_str)))
        .withColumn("CreatedBy",     pick(USERS, F.col("id"), "CreatedBy"))
        .withColumn("LastUpdatedAt", F.to_timestamp(F.lit(run_date_str)))
    )

    client_write_df = client_df.drop("id", "BranchID")

    # >>> DQ INJECT: 1% NULL State
    client_write_df = client_write_df.withColumn(
        "State",
        F.when(F.rand(seed=DQ_SEED_NULL_STATE) < DQ_NULL_STATE_RATE, F.lit(None))
         .otherwise(F.col("State"))
    )

    # ---- t_AccountCustomer (loan accounts only) ----
    account_raw = (
        client_df
        .withColumn("_n_ln",  F.when(F.col("id") % 2 == 0, F.lit(2)).otherwise(F.lit(1)))
        .withColumn("_slots", F.sequence(F.lit(1), F.col("_n_ln")))
        .withColumn("_slot",  F.explode("_slots"))
        .withColumn("_acc_global_seq",
                    F.lit(ACC_SEQ_START)
                    + (F.col("id") - F.lit(CLIENT_ID_START)) * F.lit(2)
                    + F.col("_slot") - F.lit(1))
        .withColumn("AccountID",
                    F.concat(F.col("BranchID"),
                             F.lit("LN"),
                             F.lpad(F.col("_acc_global_seq").cast(StringType()), 4, "0")))
        .withColumn("AccountType",
                    F.when(F.col("_slot") == 1, "LOAN_PRIMARY").otherwise("LOAN_SECONDARY"))
        .withColumn("ProductID",
                    F.when(F.col("_slot") == 1,
                           pick(["PL001","PL002","HL001","HL002","GL001"], F.col("id")))
                     .otherwise(pick(["GL001","BL001","PL002"], F.col("id") + F.lit(3))))
        .withColumn("ClearBalance",  F.round(F.pow(F.lit(10),
                                                   F.lit(3) + (F.col("id") % 100) / F.lit(25)), 2))
        .withColumn("FreezeAmount",  F.round(F.col("ClearBalance") * (F.col("id") % 10) / F.lit(100), 2))
        .withColumn("AccountStatus", F.when(F.col("id") % 50 == 0, "CLOSED")
                                      .when(F.col("id") % 25 == 0, "DORMANT")
                                      .when(F.col("id") % 15 == 0, "FROZEN")
                                      .otherwise("ACTIVE"))
        .withColumn("CreatedAt",     F.to_timestamp(F.lit(run_date_str)))
        .withColumn("CreatedBy",     pick(USERS, F.col("id"), "CreatedBy"))
        .withColumn("LastUpdatedAt", F.to_timestamp(F.lit(run_date_str)))
    )

    account_write_df = account_raw.select(
        "OurBranchID", "ClientID", "AccountID", "ProductID",
        "AccountType", "ClearBalance", "FreezeAmount",
        "AccountStatus", "CreatedAt", "CreatedBy", "LastUpdatedAt"
    )

    # ---- t_Loan ----
    ln_accounts = (
        account_raw
        .withColumn("_n_series",   F.when(F.col("id") % 3 == 0, F.lit(2)).otherwise(F.lit(1)))
        .withColumn("_series_arr", F.sequence(F.lit(1), F.col("_n_series")))
        .withColumn("LoanSeries",  F.explode("_series_arr"))
        .withColumn("_loan_global_seq",
                    F.lit(LOAN_SEQ_START)
                    + (F.col("id") - F.lit(CLIENT_ID_START)) * F.lit(4)
                    + F.col("LoanSeries"))
    )

    loan_df = (
        ln_accounts
        .withColumn("SanctionDate",       F.lit(run_date_str).cast(DateType()))
        .withColumn("SanctionAmount",     F.round(F.pow(F.lit(10),
                                                        F.lit(4) + (F.col("id") % 100) / F.lit(20)), 2))
        .withColumn("DisbursementDate",   F.lit(run_date_str).cast(DateType()))
        .withColumn("DisbursementAmount", F.round(F.col("SanctionAmount") * F.lit(0.95), 2))
        .withColumn("LoanStatus",         F.when(F.col("id") % 100 < 60, "ACTIVE")
                                           .when(F.col("id") % 100 < 80, "CLOSED")
                                           .when(F.col("id") % 100 < 90, "NPA")
                                           .when(F.col("id") % 100 < 95, "WRITTEN_OFF")
                                           .otherwise("RESTRUCTURED"))
        .withColumn("InterestRate",       F.round(F.lit(8) + (F.col("id") % 120) / F.lit(10), 2))
        .withColumn("Tenure",             (F.col("id") % 360 + 12).cast(IntegerType()))
        .withColumn("Frequency",          F.when(F.col("id") % 4 == 0, "QUARTERLY")
                                           .when(F.col("id") % 4 == 1, "WEEKLY")
                                           .otherwise("MONTHLY"))
        .withColumn("RepaymentType",      F.when(F.col("id") % 3 == 0, "BULLET")
                                           .when(F.col("id") % 3 == 1, "INTEREST_ONLY")
                                           .otherwise("EMI"))
        .withColumn("InterestAmount",     F.round(F.col("SanctionAmount") * F.col("InterestRate")
                                                   / F.lit(100) * F.col("Tenure") / F.lit(12), 2))
        .withColumn("OutstandingPrincipal", F.col("SanctionAmount"))
        .withColumn("OutstandingInterest",  F.col("InterestAmount"))
        .withColumn("DPD",        F.lit(0))
        .withColumn("IsNPS",      F.lit(False))
        .withColumn("ParFlag",    F.lit(False))
        .withColumn("FundID",     pick(FUND_IDS, F.col("id"), "FundID"))
        .withColumn("CreatedAt",  F.to_timestamp(F.lit(run_date_str)))
        .withColumn("CreatedBy",  pick(USERS, F.col("id"), "CreatedBy"))
        # Native MD5 — replaces Python UDF
        .withColumn("LoanID",
                    F.upper(F.md5(F.concat_ws("|",
                                              F.col("OurBranchID"),
                                              F.col("AccountID"),
                                              F.col("LoanSeries").cast(StringType())))))
        .withColumn("MaturityDate",  F.add_months(F.col("DisbursementDate"), F.col("Tenure")))
        .withColumn("LastUpdatedAt", F.to_timestamp(F.lit(run_date_str)))
    )

    loan_write_df = loan_df.select(
        "OurBranchID", "AccountID", "LoanSeries",
        "SanctionDate", "SanctionAmount", "DisbursementDate", "DisbursementAmount",
        "LoanStatus", "InterestRate", "InterestAmount", "Tenure", "Frequency",
        "RepaymentType", "OutstandingPrincipal", "OutstandingInterest",
        "DPD", "IsNPS", "ParFlag", "FundID",
        "LoanID", "MaturityDate",
        "CreatedAt", "CreatedBy", "LastUpdatedAt"
    )

    # >>> DQ INJECT: 0.5% duplicate LoanIDs
    loan_dups = loan_write_df.sample(
        withReplacement=False,
        fraction=DQ_DUP_LOAN_RATE,
        seed=DQ_SEED_DUP_LOAN
    )
    loan_write_df = loan_write_df.unionByName(loan_dups)

    # ---- t_LoanInstallment ----
    MAX_INSTALLMENTS = 36
    installment_df = (
        loan_df
        .withColumn("_tenure",         F.least(F.lit(MAX_INSTALLMENTS), F.col("Tenure")))
        .withColumn("_installments",   F.sequence(F.lit(1), F.col("_tenure")))
        .withColumn("InstallmentNo",   F.explode("_installments"))
        .withColumn("InstallmentDate", F.add_months(F.col("DisbursementDate"), F.col("InstallmentNo")))
        .withColumn("PrincipalDue",    F.round(F.col("SanctionAmount") / F.col("_tenure"), 2))
        .withColumn("InterestDue",     F.round(F.col("SanctionAmount") * F.col("InterestRate")
                                                / F.lit(100) / F.lit(12), 2))
        .withColumn("InstallmentAmount", F.round(F.col("PrincipalDue") + F.col("InterestDue"), 2))
        .withColumn("PaidStatus",      F.lit("PENDING"))
        .withColumn("TransactionDate", F.lit(None).cast(DateType()))
        .withColumn("CreatedAt",       F.to_timestamp(F.lit(run_date_str)))
        .withColumn("CreatedBy",       pick(USERS, F.col("id"), "CreatedBy"))
        .withColumn("LastUpdatedAt",   F.to_timestamp(F.lit(run_date_str)))
    )

    installment_write_df = installment_df.select(
        "OurBranchID", "AccountID", "LoanSeries", "LoanID",
        "InstallmentNo", "InstallmentDate",
        "PrincipalDue", "InterestDue", "InstallmentAmount",
        "PaidStatus", "TransactionDate",
        "CreatedAt", "CreatedBy", "LastUpdatedAt"
    )

    # ---- Write 4 reference tables (same batch # for alignment) ----
    write_batch(client_write_df,      "t_Client",          PARTITION_SUFFIX, n_partitions=10)
    write_batch(account_write_df,     "t_AccountCustomer", PARTITION_SUFFIX, n_partitions=10)
    write_batch(loan_write_df,        "t_Loan",            PARTITION_SUFFIX, n_partitions=10)
    write_batch(installment_write_df, "t_LoanInstallment", PARTITION_SUFFIX, n_partitions=20)

    # Update sequence state
    state["last_client_id"] = CLIENT_ID_END - 1
    state["last_acc_seq"]   = ACC_SEQ_START + n_new_loans * 2 - 1
    state["last_loan_seq"]  = LOAN_SEQ_START + n_new_loans * 4 - 1

    # =========================================================================
    # STEP 2 — Disbursement transactions
    # =========================================================================
    trx_disburse = (
        loan_df
        .filter(F.col("DisbursementDate") == F.lit(run_date_str).cast(DateType()))
        .select("OurBranchID", "AccountID", "LoanSeries", "LoanID", "DisbursementAmount")
        .withColumn("TrxType",            F.lit("DISBURSEMENT"))
        .withColumn("DrCr",               F.lit("CR"))
        .withColumn("Amount",             F.col("DisbursementAmount"))
        .withColumn("TrxDateTime",        F.to_timestamp(F.concat(F.lit(run_date_str), F.lit(" 10:00:00"))))
        .withColumn("ValueDate",          F.lit(run_date_str).cast(DateType()))
        .withColumn("PrincipalComponent", F.lit(0.0))
        .withColumn("InterestComponent",  F.lit(0.0))
        .withColumn("PaymentMode", pick(PAYMENT_MODES, F.crc32(F.col("AccountID")), "PaymentMode"))
        .withColumn("CreatedAt",          F.col("TrxDateTime"))
        .withColumn("CreatedBy",   pick(USERS,         F.crc32(F.col("AccountID")), "CreatedBy"))
        .withColumn("LastUpdatedAt",      F.col("TrxDateTime"))
        .select("OurBranchID", "AccountID", "LoanID", "LoanSeries",
                "TrxType", "DrCr", "Amount", "TrxDateTime", "ValueDate",
                "PrincipalComponent", "InterestComponent",
                "PaymentMode", "CreatedAt", "CreatedBy", "LastUpdatedAt")
    )

    # =========================================================================
    # STEP 3 — Repayment transactions (80% of due installments)
    # =========================================================================
    trx_list = [trx_disburse]

    inst_path = f"{OUTPUT_PATH}/t_LoanInstallment"
    if path_exists(inst_path):
        all_installments_df = spark.read.parquet(inst_path)
        due_installments = all_installments_df.filter(
            F.col("InstallmentDate") == F.lit(run_date_str).cast(DateType())
        )

        trx_repayment = (
            due_installments
            .filter(F.rand(seed=RAND_SEED_REPAYMENT) <= F.lit(REPAYMENT_RATE))
            .select("OurBranchID", "AccountID", "LoanSeries", "LoanID",
                    "PrincipalDue", "InterestDue", "InstallmentAmount")
            .withColumn("TrxType",            F.lit("REPAYMENT"))
            .withColumn("DrCr",               F.lit("DR"))
            .withColumn("Amount",             F.col("InstallmentAmount"))
            .withColumn("TrxDateTime",        F.to_timestamp(F.concat(F.lit(run_date_str), F.lit(" 14:00:00"))))
            .withColumn("ValueDate",          F.lit(run_date_str).cast(DateType()))
            .withColumn("PrincipalComponent", F.col("PrincipalDue"))
            .withColumn("InterestComponent",  F.col("InterestDue"))
            .withColumn("PaymentMode", pick(PAYMENT_MODES, F.crc32(F.col("AccountID")), "PaymentMode"))
            .withColumn("CreatedAt",          F.col("TrxDateTime"))
            .withColumn("CreatedBy",   pick(USERS,         F.crc32(F.col("AccountID")), "CreatedBy"))
            .withColumn("LastUpdatedAt",      F.col("TrxDateTime"))
            .select("OurBranchID", "AccountID", "LoanID", "LoanSeries",
                    "TrxType", "DrCr", "Amount", "TrxDateTime", "ValueDate",
                    "PrincipalComponent", "InterestComponent",
                    "PaymentMode", "CreatedAt", "CreatedBy", "LastUpdatedAt")
        )
        trx_list.append(trx_repayment)
    else:
        print(f"   No installments yet → skipping repayments")

    # =========================================================================
    # STEP 4 — Combine, assign hash-based TrxID, inject DQ, write
    # =========================================================================
    trx_combined = trx_list[0]
    for t in trx_list[1:]:
        trx_combined = trx_combined.unionByName(t)

    # >>> Hash-based TrxID — no Window, fully parallel
    # MD5 over the natural key gives 32 hex chars; take first 14 → "TRX" + 14 hex
    trx_combined = trx_combined.withColumn(
        "TrxID",
        F.concat(
            F.lit("TRX"),
            F.upper(F.substring(
                F.md5(F.concat_ws("|",
                                  F.col("AccountID"),
                                  F.col("LoanID"),
                                  F.col("LoanSeries").cast(StringType()),
                                  F.col("TrxType"),
                                  F.col("TrxDateTime").cast(StringType()))),
                1, 14
            ))
        )
    )

    # >>> DQ INJECT: 0.1% malformed AccountID
    trx_combined = trx_combined.withColumn(
        "AccountID",
        F.when(F.rand(seed=DQ_SEED_MALFORMED_ACCID) < DQ_MALFORMED_ACCID_RATE,
               F.substring(F.col("AccountID"), 1, 9))
         .otherwise(F.col("AccountID"))
    )

    # >>> DQ INJECT: 0.05% future-dated CreatedAt
    trx_combined = trx_combined.withColumn(
        "CreatedAt",
        F.when(F.rand(seed=DQ_SEED_FUTURE_CREATEDAT) < DQ_FUTURE_CREATEDAT_RATE,
               F.col("CreatedAt") + F.expr("INTERVAL 30 DAYS"))
         .otherwise(F.col("CreatedAt"))
    )

    trx_combined = trx_combined.select(
        "OurBranchID", "AccountID", "LoanID", "LoanSeries",
        "TrxID", "TrxDateTime", "ValueDate", "TrxType", "DrCr", "Amount",
        "PrincipalComponent", "InterestComponent",
        "PaymentMode", "CreatedAt", "CreatedBy", "LastUpdatedAt"
    )

    write_batch(trx_combined, "t_AccountTrx", PARTITION_SUFFIX, n_partitions=10)

    state["total_runs"] += 1

# Persist state once after all dates processed
write_state(state)

print("\n" + "=" * 60)
print(f"  COMPLETE — {len(DATE_RANGE)} dates processed")
print("=" * 60)
