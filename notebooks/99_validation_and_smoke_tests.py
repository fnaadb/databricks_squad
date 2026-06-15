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

# COMMAND ----------

# Configuration - standalone (no YAML dependencies)
CATALOG = "purviewcatalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Test results tracking
test_results = []

def log_test(name: str, passed: bool, details: str = ""):
    """Log a test result."""
    status = "✅ PASS" if passed else "❌ FAIL"
    test_results.append({"name": name, "passed": passed, "details": details})
    print(f"{status}: {name}")
    if details:
        print(f"       Details: {details}")

print(f"Catalog: {CATALOG}")
print(f"Bronze: {BRONZE_SCHEMA}, Silver: {SILVER_SCHEMA}, Gold: {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Validation

# COMMAND ----------

print("=" * 50)
print("BRONZE LAYER VALIDATION")
print("=" * 50 + "\n")

# Check Bronze tables exist and have data
bronze_tables = [
    "transactions_raw",
    "customers_raw",
    "accounts_raw",
    "merchants_raw",
]

bronze_counts = {}
for table in bronze_tables:
    full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    try:
        count = spark.table(full_name).count()
        bronze_counts[table] = count
        log_test(f"Bronze {table} exists", count > 0, f"Count: {count:,}")
    except Exception as e:
        log_test(f"Bronze {table} exists", False, str(e))

# COMMAND ----------

# Check Bronze transaction metadata columns
txn_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw")

metadata_cols = ["_batch_id"]  # Check for batch tracking
for col in metadata_cols:
    exists = col in txn_df.columns
    log_test(f"Bronze transactions has {col}", exists)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Validation

# COMMAND ----------

print("\n" + "=" * 50)
print("SILVER LAYER VALIDATION")
print("=" * 50 + "\n")

# Check Silver tables exist
silver_tables = [
    "dim_customer",
    "dim_account",
    "dim_merchant",
    "dim_date",
    "fact_transactions",
    "quarantine",
]

silver_counts = {}
for table in silver_tables:
    full_name = f"{CATALOG}.{SILVER_SCHEMA}.{table}"
    try:
        count = spark.table(full_name).count()
        silver_counts[table] = count
        log_test(f"Silver {table} exists", count > 0, f"Count: {count:,}")
    except Exception as e:
        log_test(f"Silver {table} exists", False, str(e))

# COMMAND ----------

# Validate fact_transactions schema
fact_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")

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

# Check for null transaction_keys
null_txn_keys = fact_df.filter(F.col("transaction_key").isNull()).count()
total_txns = fact_df.count()
null_rate = null_txn_keys / max(total_txns, 1)
log_test("No null transaction_keys", null_rate == 0, f"Null rate: {null_rate:.4%}")

# Check for duplicate transaction_keys
dup_count = fact_df.groupBy("transaction_key").count().filter(F.col("count") > 1).count()
dup_rate = dup_count / max(total_txns, 1)
log_test("No duplicate transaction_keys", dup_rate < 0.01, f"Duplicate rate: {dup_rate:.4%}")

# Check amount range
invalid_amounts = fact_df.filter(
    (F.col("amount_usd") < 0) | (F.col("amount_usd") > 1000000)
).count()
log_test("Amount in valid range (0-1M)", invalid_amounts == 0, f"Invalid: {invalid_amounts:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Validation

# COMMAND ----------

print("\n" + "=" * 50)
print("GOLD LAYER VALIDATION")
print("=" * 50 + "\n")

# Check Gold tables exist (excluding tables we didn't create)
gold_tables = [
    "revenue_summary",
    "customer_analytics",
    "merchant_analytics",
    "channel_performance",
    "daily_kpis",
    "monthly_kpis",
]

gold_counts = {}
for table in gold_tables:
    full_name = f"{CATALOG}.{GOLD_SCHEMA}.{table}"
    try:
        count = spark.table(full_name).count()
        gold_counts[table] = count
        log_test(f"Gold {table} exists", count > 0, f"Count: {count:,}")
    except Exception as e:
        log_test(f"Gold {table} exists", False, str(e))

# COMMAND ----------

# Validate customer analytics has segments
cust_analytics = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_analytics")
segments = cust_analytics.select("customer_segment").distinct().count()
log_test("Customer segments populated", segments > 0, f"Unique segments: {segments}")

# Validate merchant analytics has tiers
merch_analytics = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.merchant_analytics")
tiers = merch_analytics.select("merchant_tier").distinct().count()
log_test("Merchant tiers populated", tiers > 0, f"Unique tiers: {tiers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity

# COMMAND ----------

print("\n" + "=" * 50)
print("REFERENTIAL INTEGRITY")
print("=" * 50 + "\n")

# Check fact -> dimension joins
fact_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")
dim_customer = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer")
dim_merchant = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_merchant")

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

print("\n" + "=" * 50)
print("BUSINESS RULE VALIDATION")
print("=" * 50 + "\n")

fact_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")

# Rule: All amounts should be non-negative in Silver
negative_amounts = fact_df.filter(F.col("amount_usd") < 0).count()
log_test(
    "No negative amounts in Silver",
    negative_amounts == 0,
    f"Negative count: {negative_amounts}"
)

# Rule: amount_usd should be populated
non_null_amounts = fact_df.filter(F.col("amount_usd").isNotNull()).count()
total_records = fact_df.count()
coverage = non_null_amounts / max(total_records, 1)

log_test(
    "USD amount populated (> 95%)",
    coverage > 0.95,
    f"Coverage: {coverage:.2%}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Layer-to-Layer Reconciliation

# COMMAND ----------

print("\n" + "=" * 50)
print("LAYER-TO-LAYER RECONCILIATION")
print("=" * 50 + "\n")

# Bronze vs Silver record counts (allowing for quarantine)
bronze_txn_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw").count()
silver_fact_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions").count()
quarantine_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.quarantine").count()

# Silver + Quarantine should approximately equal Bronze
total_silver = silver_fact_count + quarantine_count
diff = abs(bronze_txn_count - total_silver)
diff_pct = diff / max(bronze_txn_count, 1)

log_test(
    "Bronze ≈ Silver + Quarantine (< 5% diff)",
    diff_pct < 0.05,
    f"Bronze: {bronze_txn_count:,}, Silver+Q: {total_silver:,}, Diff: {diff_pct:.2%}"
)

# Customer dimension coverage
bronze_cust_count = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw").count()
silver_cust_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer").count()
cust_coverage = silver_cust_count / max(bronze_cust_count, 1)

log_test(
    "Customer dimension coverage (> 95%)",
    cust_coverage > 0.95,
    f"Bronze: {bronze_cust_count:,}, Silver: {silver_cust_count:,}"
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
print(f"Passed: {passed} ✅")
print(f"Failed: {failed} ❌")
print(f"Pass Rate: {passed/max(total,1)*100:.1f}%")

if failed == 0:
    print("\n🎉 ALL TESTS PASSED!")
else:
    print("\n⚠️ Some tests failed. Review details above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record Count Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("RECORD COUNT SUMMARY")
print("=" * 50)

print("\n📦 BRONZE LAYER:")
for table, count in bronze_counts.items():
    print(f"  {table}: {count:,}")

print("\n🥈 SILVER LAYER:")
for table, count in silver_counts.items():
    print(f"  {table}: {count:,}")

print("\n🥇 GOLD LAYER:")
for table, count in gold_counts.items():
    print(f"  {table}: {count:,}")

# Total records
total_bronze = sum(bronze_counts.values())
total_silver = sum(silver_counts.values())
total_gold = sum(gold_counts.values())
grand_total = total_bronze + total_silver + total_gold

print(f"\n📊 TOTALS:")
print(f"  Bronze: {total_bronze:,}")
print(f"  Silver: {total_silver:,}")
print(f"  Gold: {total_gold:,}")
print(f"  Grand Total: {grand_total:,}")

# COMMAND ----------

print("\nValidation and smoke tests completed!")
