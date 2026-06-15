# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformations
# MAGIC 
# MAGIC Transforms Bronze raw data into Silver curated tables.
# MAGIC 
# MAGIC ## Process Overview
# MAGIC 1. Read Bronze raw tables
# MAGIC 2. Validate and filter data quality
# MAGIC 3. Apply business transformations
# MAGIC 4. Build dimension and fact tables
# MAGIC 5. Write to Silver Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import sys

# Add src to path
sys.path.insert(0, "../")

from src.common.config import load_config
from src.common.logging_utils import get_logger
from src.silver.silver_financials import SilverFinancialsTransformer
from src.silver.silver_dimensions import SilverDimensionsTransformer
from src.silver.quarantine import QuarantineHandler

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load configuration
config = load_config()
logger = get_logger("silver_transforms")

print(f"Environment: {config.environment}")
print(f"Catalog: {config.catalog.name}")
print(f"Silver Schema: {config.schemas.silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Transformers

# COMMAND ----------

# Initialize transformers
financials_transformer = SilverFinancialsTransformer(spark, config)
dimensions_transformer = SilverDimensionsTransformer(spark, config)
quarantine_handler = QuarantineHandler(spark, config)

# Ensure tables exist
financials_transformer.create_table_if_not_exists()
dimensions_transformer.create_all_tables_if_not_exist()
quarantine_handler.create_table_if_not_exists()

print("Silver tables created/verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension Tables
# MAGIC 
# MAGIC Dimensions are built first since fact tables reference them.

# COMMAND ----------

# Build date dimension (static, full rebuild)
print("Building dim_date...")
date_count = dimensions_transformer.build_dim_date(
    start_year=2020,
    end_year=2030,
)
print(f"Built dim_date with {date_count} records")

# COMMAND ----------

# Build customer dimension (SCD Type 2)
print("Building dim_customer...")
customer_count = dimensions_transformer.build_dim_customer(scd_type=2)
print(f"Built dim_customer with {customer_count} records")

# COMMAND ----------

# Build account dimension (SCD Type 2)
print("Building dim_account...")
account_count = dimensions_transformer.build_dim_account(scd_type=2)
print(f"Built dim_account with {account_count} records")

# COMMAND ----------

# Build merchant dimension (SCD Type 1)
print("Building dim_merchant...")
merchant_count = dimensions_transformer.build_dim_merchant()
print(f"Built dim_merchant with {merchant_count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Fact Tables

# COMMAND ----------

# Transform transactions (incremental)
print("Transforming transactions...")
report = financials_transformer.transform_transactions(incremental=True)

print(f"\n=== Transformation Report ===")
print(f"Total Input Records: {report.total_rows:,}")
print(f"Quality Checks: {len(report.checks)}")
print(f"Overall Passed: {report.overall_passed}")

# COMMAND ----------

# Get processing statistics
stats = financials_transformer.get_processing_stats()

print("\n=== Silver Transactions Statistics ===")
for key, value in stats.items():
    print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Quarantine

# COMMAND ----------

# Check quarantine statistics
quarantine_stats = quarantine_handler.get_quarantine_stats()

print("\n=== Quarantine Statistics ===")
print(f"Total Quarantined: {quarantine_stats.get('total_records', 0):,}")

if quarantine_stats.get('by_reason'):
    print("\nBy Rejection Reason:")
    for reason in quarantine_stats['by_reason'][:5]:
        print(f"  {reason['reason']}: {reason['count']}")

# COMMAND ----------

# Preview quarantined records
quarantined_df = quarantine_handler.get_quarantined_records(limit=10)
display(quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Tables

# COMMAND ----------

# Verify record counts
print("\n=== Silver Layer Summary ===")

silver_tables = [
    (f"{config.catalog.name}.{config.schemas.silver}.dim_customer", "dim_customer"),
    (f"{config.catalog.name}.{config.schemas.silver}.dim_account", "dim_account"),
    (f"{config.catalog.name}.{config.schemas.silver}.dim_merchant", "dim_merchant"),
    (f"{config.catalog.name}.{config.schemas.silver}.dim_date", "dim_date"),
    (f"{config.catalog.name}.{config.schemas.silver}.fact_transactions", "fact_transactions"),
    (f"{config.catalog.name}.{config.schemas.silver}.quarantine", "quarantine"),
]

for table_name, label in silver_tables:
    try:
        count = spark.table(table_name).count()
        print(f"{label}: {count:,} records")
    except Exception as e:
        print(f"{label}: Error - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# Preview fact_transactions
display(
    spark.table(f"{config.catalog.name}.{config.schemas.silver}.fact_transactions")
    .orderBy(F.desc("_created_timestamp"))
    .limit(10)
)

# COMMAND ----------

# Preview customer dimension
display(
    spark.table(f"{config.catalog.name}.{config.schemas.silver}.dim_customer")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Calculate data quality metrics
fact_df = spark.table(f"{config.catalog.name}.{config.schemas.silver}.fact_transactions")

quality_metrics = fact_df.agg(
    F.count("*").alias("total_records"),
    F.sum(F.when(F.col("customer_key").isNull(), 1).otherwise(0)).alias("null_customer_key"),
    F.sum(F.when(F.col("amount_usd").isNull(), 1).otherwise(0)).alias("null_amount_usd"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.countDistinct("merchant_key").alias("unique_merchants"),
    F.min("transaction_timestamp").alias("earliest_txn"),
    F.max("transaction_timestamp").alias("latest_txn"),
).collect()[0]

print("\n=== Data Quality Metrics ===")
print(f"Total Records: {quality_metrics.total_records:,}")
print(f"Null Customer Keys: {quality_metrics.null_customer_key}")
print(f"Null Amount USD: {quality_metrics.null_amount_usd}")
print(f"Unique Customers: {quality_metrics.unique_customers:,}")
print(f"Unique Merchants: {quality_metrics.unique_merchants:,}")
print(f"Date Range: {quality_metrics.earliest_txn} to {quality_metrics.latest_txn}")

# COMMAND ----------

print("Silver transformation completed successfully!")
