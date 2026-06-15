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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType, ByteType, BooleanType, DateType, TimestampType, DecimalType
from pyspark.sql.window import Window
from datetime import datetime, date, timedelta
import hashlib

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration - standalone (no YAML dependencies)
CATALOG = "purviewcatalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

print(f"Catalog: {CATALOG}")
print(f"Bronze Schema: {BRONZE_SCHEMA}")
print(f"Silver Schema: {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Silver Tables (Clean Slate)

# COMMAND ----------

# Drop existing tables to avoid schema conflicts
tables_to_drop = ["dim_customer", "dim_account", "dim_merchant", "dim_date", "fact_transactions", "quarantine"]

for table in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SILVER_SCHEMA}.{table}")
        print(f"Dropped {table}")
    except Exception as e:
        print(f"Could not drop {table}: {e}")

print("Silver tables cleared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension Tables

# COMMAND ----------

# Build date dimension (2020-2030)
print("Building dim_date...")

start_date = date(2020, 1, 1)
end_date = date(2030, 12, 31)
date_list = []

# Define explicit schema for dim_date to ensure proper Databricks data types
DIM_DATE_SCHEMA = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("full_date", DateType(), False),
    StructField("year", ShortType(), False),
    StructField("quarter", ByteType(), False),
    StructField("month", ByteType(), False),
    StructField("month_name", StringType(), False),
    StructField("week_of_year", ByteType(), False),
    StructField("day_of_month", ByteType(), False),
    StructField("day_of_week", ByteType(), False),
    StructField("day_name", StringType(), False),
    StructField("is_weekend", BooleanType(), False),
    StructField("is_holiday", BooleanType(), False),
])

current = start_date
while current <= end_date:
    date_key = int(current.strftime("%Y%m%d"))
    date_list.append((
        date_key,
        current,
        current.year,
        (current.month - 1) // 3 + 1,
        current.month,
        current.strftime("%B"),
        current.isocalendar()[1],
        current.day,
        current.weekday() + 1,
        current.strftime("%A"),
        current.weekday() >= 5,
        False
    ))
    current += timedelta(days=1)

date_df = spark.createDataFrame(date_list, schema=DIM_DATE_SCHEMA)
date_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_date")
print(f"Built dim_date with {len(date_list):,} records")

# COMMAND ----------

# Build customer dimension from bronze
# Bronze schema: customer_id, first_name, last_name, email, phone, date_of_birth, country_code, created_at, updated_at
print("Building dim_customer...")

customers_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw")

# Show bronze schema for debugging
print("Bronze customers schema:")
customers_bronze.printSchema()

# Cleanse and transform - using actual bronze columns
# Use expr("try_cast") for safe date parsing (tolerates malformed dates)
dim_customer = customers_bronze.filter(
    F.col("customer_id").isNotNull()
).select(
    F.monotonically_increasing_id().alias("customer_key"),
    F.col("customer_id"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("email"),
    F.col("phone"),
    F.expr("try_cast(date_of_birth as date)").alias("date_of_birth"),
    F.col("country_code"),
    F.expr("try_cast(created_at as date)").alias("customer_since"),
    F.lit(True).alias("is_active"),  # Default to active since no status in bronze
    F.current_timestamp().alias("effective_start_date"),
    F.lit(None).cast(TimestampType()).alias("effective_end_date"),
    F.lit(True).alias("is_current"),
    F.current_timestamp().alias("_created_timestamp")
)

dim_customer.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer")
customer_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer").count()
print(f"Built dim_customer with {customer_count:,} records")

# COMMAND ----------

# Build account dimension
# Bronze schema: account_id, customer_id, account_type, account_number, balance, currency, status, opened_date, closed_date
print("Building dim_account...")

accounts_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.accounts_raw")

# Show bronze schema for debugging
print("Bronze accounts schema:")
accounts_bronze.printSchema()

# Get customer keys for lookup
customer_lookup = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer").select("customer_key", "customer_id")

dim_account = accounts_bronze.filter(
    F.col("account_id").isNotNull()
).join(
    customer_lookup, "customer_id", "left"
).select(
    F.monotonically_increasing_id().alias("account_key"),
    F.col("account_id"),
    F.col("customer_key"),
    F.col("account_type"),
    F.col("status").alias("account_status"),
    F.expr("try_cast(balance as decimal(18,2))").alias("balance"),
    F.col("currency"),
    F.expr("try_cast(opened_date as date)").alias("opened_date"),
    F.expr("try_cast(closed_date as date)").alias("closed_date"),
    F.current_timestamp().alias("effective_start_date"),
    F.lit(None).cast(TimestampType()).alias("effective_end_date"),
    F.lit(True).alias("is_current"),
    F.current_timestamp().alias("_created_timestamp")
)

dim_account.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_account")
account_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_account").count()
print(f"Built dim_account with {account_count:,} records")

# COMMAND ----------

# Build merchant dimension
# Bronze schema: merchant_id, merchant_name, category, country_code, city, mcc_code
print("Building dim_merchant...")

merchants_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.merchants_raw")

# Show bronze schema for debugging
print("Bronze merchants schema:")
merchants_bronze.printSchema()

dim_merchant = merchants_bronze.filter(
    F.col("merchant_id").isNotNull()
).select(
    F.monotonically_increasing_id().alias("merchant_key"),
    F.col("merchant_id"),
    F.col("merchant_name"),
    F.col("category").alias("merchant_category"),
    F.col("city"),
    F.col("country_code"),
    F.col("mcc_code"),
    F.lit(True).alias("is_active"),  # Default to active since no status in bronze
    F.current_timestamp().alias("_created_timestamp")
)

dim_merchant.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.dim_merchant")
merchant_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_merchant").count()
print(f"Built dim_merchant with {merchant_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Fact Table

# COMMAND ----------

# Build fact_transactions
# Bronze schema: transaction_id, customer_id, account_id, merchant_id, transaction_date, transaction_timestamp, 
#                amount, currency, transaction_type, status, channel, country_code, description
print("Building fact_transactions...")

transactions_bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw")

# Show bronze schema for debugging
print("Bronze transactions schema:")
transactions_bronze.printSchema()

# Get dimension keys
customer_lookup = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer").select("customer_key", "customer_id")
account_lookup = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_account").select("account_key", "account_id")
merchant_lookup = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_merchant").select("merchant_key", "merchant_id")

# Data quality filters
valid_currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]

# Filter valid transactions - use try_cast for safe numeric parsing
valid_transactions = transactions_bronze.filter(
    (F.col("transaction_id").isNotNull()) &
    (F.expr("try_cast(amount as decimal(18,2))") > 0) &
    (F.col("currency").isin(valid_currencies)) &
    (F.col("customer_id").isNotNull())
)

# Filter invalid for quarantine
invalid_transactions = transactions_bronze.filter(
    (F.col("transaction_id").isNull()) |
    (F.expr("try_cast(amount as decimal(18,2))") <= 0) |
    (F.expr("try_cast(amount as decimal(18,2))").isNull()) |
    (~F.col("currency").isin(valid_currencies)) |
    (F.col("customer_id").isNull())
)

# Build fact table with dimension keys
# Use try_cast and try_to_timestamp to handle malformed dates gracefully
fact_transactions = valid_transactions.join(
    customer_lookup, "customer_id", "left"
).join(
    account_lookup, "account_id", "left"
).join(
    merchant_lookup, "merchant_id", "left"
).select(
    F.monotonically_increasing_id().alias("transaction_key"),
    F.col("transaction_id"),
    F.col("customer_key"),
    F.col("account_key"),
    F.col("merchant_key"),
    F.expr("cast(date_format(try_cast(transaction_date as date), 'yyyyMMdd') as int)").alias("date_key"),
    F.expr("try_to_timestamp(transaction_timestamp)").alias("transaction_timestamp"),
    F.expr("try_cast(amount as decimal(18,2))").alias("amount_usd"),
    F.col("currency"),
    F.col("transaction_type"),
    F.col("channel"),
    F.when(F.expr("try_cast(amount as decimal(18,2))") > 10000, True).otherwise(False).alias("is_fraud"),
    F.lit(f"silver_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}").alias("_batch_id"),
    F.current_timestamp().alias("_created_timestamp")
)

fact_transactions.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")
txn_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions").count()

# Quarantine bad records
quarantine_count = 0
if invalid_transactions.count() > 0:
    quarantine_df = invalid_transactions.select(
        F.col("transaction_id").alias("record_id"),
        F.lit("transactions_raw").alias("source_table"),
        F.when(F.col("amount").cast("decimal(18,2)") <= 0, "Negative/zero amount")
         .when(~F.col("currency").isin(valid_currencies), "Invalid currency")
         .when(F.col("customer_id").isNull(), "Missing customer_id")
         .otherwise("Other validation failure").alias("rejection_reason"),
        F.to_json(F.struct("*")).alias("original_data"),
        F.current_timestamp().alias("_quarantine_timestamp")
    )
    quarantine_df.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.quarantine")
    quarantine_count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.quarantine").count()

print(f"\n=== Transformation Report ===")
print(f"Valid Records: {txn_count:,}")
print(f"Quarantined Records: {quarantine_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Tables

# COMMAND ----------

# Verify record counts
print("\n=== Silver Layer Summary ===")

silver_tables = [
    (f"{CATALOG}.{SILVER_SCHEMA}.dim_customer", "dim_customer"),
    (f"{CATALOG}.{SILVER_SCHEMA}.dim_account", "dim_account"),
    (f"{CATALOG}.{SILVER_SCHEMA}.dim_merchant", "dim_merchant"),
    (f"{CATALOG}.{SILVER_SCHEMA}.dim_date", "dim_date"),
    (f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions", "fact_transactions"),
    (f"{CATALOG}.{SILVER_SCHEMA}.quarantine", "quarantine"),
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
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")
    .orderBy(F.desc("_created_timestamp"))
    .limit(10)
)

# COMMAND ----------

# Preview customer dimension
display(
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# Calculate data quality metrics
fact_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")

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
