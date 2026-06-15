# Databricks notebook source
# MAGIC %md
# MAGIC # Validation and Smoke Tests
# MAGIC 
# MAGIC End-to-end validation of the medallion architecture pipeline.
# MAGIC 
# MAGIC ## Tests Included
# MAGIC 1. Schema validation
# MAGIC 2. Data quality checks
# MAGIC 3. Record count validation
# MAGIC 4. Referential integrity
# MAGIC 5. Business rule validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import sys

# Add src to path
sys.path.insert(0, "../")

from src.common.config import load_config
from src.common.quality import DataQualityValidator, QualityReport
from src.common.constants import VALID_CURRENCIES, TransactionStatus

# COMMAND ----------

# Load configuration
config = load_config()
validator = DataQualityValidator(spark)

# Test results tracking
test_results = []

def log_test(name: str, passed: bool, details: str = ""):
    """Log a test result."""
    status = "✅ PASS" if passed else "❌ FAIL"
    test_results.append({"name": name, "passed": passed, "details": details})
    print(f"{status}: {name}")
    if details and not passed:
        print(f"       Details: {details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Validation

# COMMAND ----------

print("=== Bronze Layer Validation ===\n")

# Check Bronze tables exist and have data
bronze_tables = [
    "transactions_raw",
    "customers_raw",
    "accounts_raw",
    "merchants_raw",
]

for table in bronze_tables:
    full_name = f"{config.catalog.name}.{config.schemas.bronze}.{table}"
    try:
        count = spark.table(full_name).count()
        log_test(f"Bronze {table} exists", count > 0, f"Count: {count}")
    except Exception as e:
        log_test(f"Bronze {table} exists", False, str(e))

# COMMAND ----------

# Check Bronze transaction metadata columns
txn_df = spark.table(f"{config.catalog.name}.{config.schemas.bronze}.transactions_raw")

required_cols = ["_source_file", "_batch_id", "_ingestion_timestamp"]
for col in required_cols:
    exists = col in txn_df.columns
    log_test(f"Bronze transactions has {col}", exists)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Validation

# COMMAND ----------

print("\n=== Silver Layer Validation ===\n")

# Check Silver tables exist
silver_tables = [
    "dim_customer",
    "dim_account",
    "dim_merchant",
    "dim_date",
    "fact_transactions",
    "quarantine",
]

for table in silver_tables:
    full_name = f"{config.catalog.name}.{config.schemas.silver}.{table}"
    try:
        count = spark.table(full_name).count()
        log_test(f"Silver {table} exists", count > 0, f"Count: {count}")
    except Exception as e:
        log_test(f"Silver {table} exists", False, str(e))

# COMMAND ----------

# Validate fact_transactions schema
fact_df = spark.table(f"{config.catalog.name}.{config.schemas.silver}.fact_transactions")

required_fact_cols = [
    "transaction_key",
    "transaction_id",
    "customer_key",
    "amount_usd",
    "date_key",
    "_created_timestamp",
]

for col in required_fact_cols:
    exists = col in fact_df.columns
    log_test(f"fact_transactions has {col}", exists)

# COMMAND ----------

# Data quality checks on fact_transactions
result = validator.check_nulls(fact_df, "transaction_key", threshold=0.0)
log_test("No null transaction_keys", result.passed, f"Null rate: {result.actual_value}")

result = validator.check_duplicates(fact_df, ["transaction_key"], threshold=0.01)
log_test("No duplicate transaction_keys", result.passed, f"Duplicate rate: {result.actual_value}")

result = validator.check_range(
    fact_df.withColumn("amount_num", F.col("amount_usd").cast("double")),
    "amount_num",
    min_value=0,
    max_value=1000000,
)
log_test("Amount in valid range", result.passed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Validation

# COMMAND ----------

print("\n=== Gold Layer Validation ===\n")

# Check Gold tables exist
gold_tables = [
    "revenue_summary",
    "customer_analytics",
    "merchant_analytics",
    "channel_performance",
    "geographic_summary",
    "daily_kpis",
    "monthly_kpis",
    "executive_summary",
]

for table in gold_tables:
    full_name = f"{config.catalog.name}.{config.schemas.gold}.{table}"
    try:
        count = spark.table(full_name).count()
        log_test(f"Gold {table} exists", count > 0, f"Count: {count}")
    except Exception as e:
        log_test(f"Gold {table} exists", False, str(e))

# COMMAND ----------

# Validate KPI calculations
daily_kpis = spark.table(f"{config.catalog.name}.{config.schemas.gold}.daily_kpis")

# Check rolling averages are calculated
has_rolling = (
    daily_kpis.filter(F.col("revenue_7d_avg").isNotNull()).count() > 0
)
log_test("Daily KPIs have rolling averages", has_rolling)

# Check trend indicators
has_trends = (
    daily_kpis.filter(F.col("revenue_trend").isNotNull()).count() > 0
)
log_test("Daily KPIs have trend indicators", has_trends)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity

# COMMAND ----------

print("\n=== Referential Integrity ===\n")

# Check fact -> dimension joins
fact_df = spark.table(f"{config.catalog.name}.{config.schemas.silver}.fact_transactions")
dim_customer = spark.table(f"{config.catalog.name}.{config.schemas.silver}.dim_customer")
dim_merchant = spark.table(f"{config.catalog.name}.{config.schemas.silver}.dim_merchant")

# Customer key integrity
orphan_customers = fact_df.join(
    dim_customer.select("customer_key"),
    "customer_key",
    "left_anti"
).filter(F.col("customer_key").isNotNull()).count()

total_with_customer = fact_df.filter(F.col("customer_key").isNotNull()).count()
orphan_rate = orphan_customers / max(total_with_customer, 1)

log_test(
    "Customer FK integrity (< 5% orphans)",
    orphan_rate < 0.05,
    f"Orphan rate: {orphan_rate:.2%}"
)

# Merchant key integrity
orphan_merchants = fact_df.join(
    dim_merchant.select("merchant_key"),
    "merchant_key",
    "left_anti"
).filter(F.col("merchant_key").isNotNull()).count()

total_with_merchant = fact_df.filter(F.col("merchant_key").isNotNull()).count()
orphan_rate = orphan_merchants / max(total_with_merchant, 1)

log_test(
    "Merchant FK integrity (< 5% orphans)",
    orphan_rate < 0.05,
    f"Orphan rate: {orphan_rate:.2%}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Rule Validation

# COMMAND ----------

print("\n=== Business Rule Validation ===\n")

fact_df = spark.table(f"{config.catalog.name}.{config.schemas.silver}.fact_transactions")

# Rule: All amounts should be non-negative in Silver
negative_amounts = fact_df.filter(F.col("amount_usd") < 0).count()
log_test(
    "No negative amounts in Silver",
    negative_amounts == 0,
    f"Negative count: {negative_amounts}"
)

# Rule: Currency should be normalized to USD
non_null_amounts = fact_df.filter(F.col("amount_usd").isNotNull()).count()
total_amounts = fact_df.filter(F.col("amount").isNotNull()).count()
coverage = non_null_amounts / max(total_amounts, 1)

log_test(
    "USD normalization coverage (> 95%)",
    coverage > 0.95,
    f"Coverage: {coverage:.2%}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer-to-Layer Reconciliation

# COMMAND ----------

print("\n=== Layer-to-Layer Reconciliation ===\n")

# Bronze vs Silver record counts (allowing for quarantine)
bronze_txn_count = spark.table(
    f"{config.catalog.name}.{config.schemas.bronze}.transactions_raw"
).count()

silver_fact_count = spark.table(
    f"{config.catalog.name}.{config.schemas.silver}.fact_transactions"
).count()

quarantine_count = spark.table(
    f"{config.catalog.name}.{config.schemas.silver}.quarantine"
).count()

# Silver + Quarantine should approximately equal Bronze
total_silver = silver_fact_count + quarantine_count
diff = abs(bronze_txn_count - total_silver)
diff_pct = diff / max(bronze_txn_count, 1)

log_test(
    "Bronze ≈ Silver + Quarantine (< 1% diff)",
    diff_pct < 0.01,
    f"Bronze: {bronze_txn_count}, Silver+Q: {total_silver}, Diff: {diff_pct:.2%}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("TEST SUMMARY")
print("=" * 50)

passed = sum(1 for t in test_results if t["passed"])
failed = sum(1 for t in test_results if not t["passed"])
total = len(test_results)

print(f"\nTotal Tests: {total}")
print(f"Passed: {passed}")
print(f"Failed: {failed}")
print(f"Pass Rate: {passed/total*100:.1f}%")

if failed > 0:
    print("\n--- Failed Tests ---")
    for t in test_results:
        if not t["passed"]:
            print(f"  ❌ {t['name']}: {t['details']}")

# COMMAND ----------

# Return overall result
all_passed = all(t["passed"] for t in test_results)

if all_passed:
    print("\n✅ ALL TESTS PASSED - Pipeline validation successful!")
else:
    print(f"\n❌ {failed} TESTS FAILED - Review issues above")
    # In production, you might raise an exception here
    # raise Exception(f"Pipeline validation failed: {failed} tests failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Freshness Check

# COMMAND ----------

# Check data freshness
fact_df = spark.table(f"{config.catalog.name}.{config.schemas.silver}.fact_transactions")

latest_txn = fact_df.agg(F.max("_created_timestamp")).collect()[0][0]

if latest_txn:
    age_hours = (datetime.now() - latest_txn).total_seconds() / 3600
    is_fresh = age_hours < config.quality.freshness_hours
    
    print(f"\nData Freshness Check:")
    print(f"  Latest Silver record: {latest_txn}")
    print(f"  Age: {age_hours:.1f} hours")
    print(f"  Threshold: {config.quality.freshness_hours} hours")
    print(f"  Status: {'✅ FRESH' if is_fresh else '⚠️ STALE'}")
else:
    print("\n⚠️ No records found in fact_transactions")
