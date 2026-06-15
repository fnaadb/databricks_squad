# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Analytics
# MAGIC 
# MAGIC Builds Gold analytics and KPI tables for business intelligence.
# MAGIC 
# MAGIC ## Process Overview
# MAGIC 1. Read Silver fact and dimension tables
# MAGIC 2. Build aggregated analytics tables
# MAGIC 3. Calculate KPIs and metrics
# MAGIC 4. Validate Gold layer output

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration - standalone (no YAML dependencies)
CATALOG = "purviewcatalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

print(f"Catalog: {CATALOG}")
print(f"Silver Schema: {SILVER_SCHEMA}")
print(f"Gold Schema: {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Existing Gold Tables (Clean Slate)

# COMMAND ----------

# Drop existing tables to avoid schema conflicts
gold_tables_to_drop = [
    "revenue_summary", 
    "customer_analytics", 
    "merchant_analytics", 
    "channel_performance",
    "daily_kpis",
    "monthly_kpis"
]

for table in gold_tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{GOLD_SCHEMA}.{table}")
        print(f"Dropped {table}")
    except Exception as e:
        print(f"Could not drop {table}: {e}")

print("Gold tables cleared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Tables

# COMMAND ----------

# Load silver tables
fact_transactions = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_transactions")
dim_customer = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_customer")
dim_account = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_account")
dim_merchant = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_merchant")
dim_date = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.dim_date")

print("Silver tables loaded:")
print(f"  fact_transactions: {fact_transactions.count():,} records")
print(f"  dim_customer: {dim_customer.count():,} records")
print(f"  dim_account: {dim_account.count():,} records")
print(f"  dim_merchant: {dim_merchant.count():,} records")
print(f"  dim_date: {dim_date.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Revenue Summary

# COMMAND ----------

print("Building revenue_summary...")

# Join fact with dimensions for revenue aggregation
revenue_summary = fact_transactions.join(
    dim_date.select("date_key", "year", "quarter", "month", "month_name"),
    "date_key",
    "left"
).join(
    dim_merchant.select("merchant_key", "merchant_category"),
    "merchant_key",
    "left"
).groupBy(
    "year", "quarter", "month", "month_name", "channel", "merchant_category"
).agg(
    F.count("transaction_key").alias("transaction_count"),
    F.sum("amount_usd").cast("decimal(18,2)").alias("total_revenue_usd"),
    F.avg("amount_usd").cast("decimal(18,2)").alias("avg_transaction_amount"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.countDistinct("merchant_key").alias("unique_merchants"),
    F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count")
).withColumn("_created_timestamp", F.current_timestamp())

revenue_summary.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.revenue_summary")
revenue_count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.revenue_summary").count()
print(f"Built revenue_summary with {revenue_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Customer Analytics

# COMMAND ----------

print("Building customer_analytics...")

# Calculate customer-level metrics
customer_metrics = fact_transactions.groupBy("customer_key").agg(
    F.count("transaction_key").alias("total_transactions"),
    F.sum("amount_usd").cast("decimal(18,2)").alias("lifetime_value_usd"),
    F.avg("amount_usd").cast("decimal(18,2)").alias("avg_transaction_amount"),
    F.min("transaction_timestamp").alias("first_transaction_date"),
    F.max("transaction_timestamp").alias("last_transaction_date"),
    F.countDistinct("merchant_key").alias("unique_merchants_visited"),
    F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count")
)

# Join with customer dimension for full analytics
customer_analytics = customer_metrics.join(
    dim_customer.select(
        "customer_key", "customer_id", "first_name", "last_name", 
        "country_code", "customer_since"
    ),
    "customer_key",
    "left"
).withColumn(
    "customer_segment",
    F.when(F.col("lifetime_value_usd") >= 10000, "Premium")
     .when(F.col("lifetime_value_usd") >= 5000, "Gold")
     .when(F.col("lifetime_value_usd") >= 1000, "Silver")
     .otherwise("Bronze")
).withColumn(
    "days_since_first_transaction",
    F.datediff(F.current_date(), F.col("first_transaction_date"))
).withColumn(
    "days_since_last_transaction",
    F.datediff(F.current_date(), F.col("last_transaction_date"))
).withColumn("_created_timestamp", F.current_timestamp())

customer_analytics.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.customer_analytics")
cust_analytics_count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_analytics").count()
print(f"Built customer_analytics with {cust_analytics_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Merchant Analytics

# COMMAND ----------

print("Building merchant_analytics...")

# Calculate merchant-level metrics
merchant_metrics = fact_transactions.groupBy("merchant_key").agg(
    F.count("transaction_key").alias("total_transactions"),
    F.sum("amount_usd").cast("decimal(18,2)").alias("total_revenue_usd"),
    F.avg("amount_usd").cast("decimal(18,2)").alias("avg_transaction_amount"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.min("transaction_timestamp").alias("first_transaction_date"),
    F.max("transaction_timestamp").alias("last_transaction_date"),
    F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count")
)

# Join with merchant dimension
merchant_analytics = merchant_metrics.join(
    dim_merchant.select(
        "merchant_key", "merchant_id", "merchant_name", 
        "merchant_category", "city", "country_code"
    ),
    "merchant_key",
    "left"
).withColumn(
    "merchant_tier",
    F.when(F.col("total_revenue_usd") >= 100000, "Enterprise")
     .when(F.col("total_revenue_usd") >= 50000, "Large")
     .when(F.col("total_revenue_usd") >= 10000, "Medium")
     .otherwise("Small")
).withColumn(
    "fraud_rate",
    F.when(F.col("total_transactions") > 0, 
           (F.col("fraud_count") / F.col("total_transactions")).cast("decimal(5,4)"))
     .otherwise(F.lit(0.0).cast("decimal(5,4)"))
).withColumn("_created_timestamp", F.current_timestamp())

merchant_analytics.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.merchant_analytics")
merch_analytics_count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.merchant_analytics").count()
print(f"Built merchant_analytics with {merch_analytics_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Channel Performance

# COMMAND ----------

print("Building channel_performance...")

channel_performance = fact_transactions.join(
    dim_date.select("date_key", "year", "month"),
    "date_key",
    "left"
).groupBy("year", "month", "channel").agg(
    F.count("transaction_key").alias("transaction_count"),
    F.sum("amount_usd").cast("decimal(18,2)").alias("total_revenue_usd"),
    F.avg("amount_usd").cast("decimal(18,2)").alias("avg_transaction_amount"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count")
).withColumn(
    "fraud_rate",
    F.when(F.col("transaction_count") > 0, 
           (F.col("fraud_count") / F.col("transaction_count")).cast("decimal(5,4)"))
     .otherwise(F.lit(0.0).cast("decimal(5,4)"))
).withColumn("_created_timestamp", F.current_timestamp())

channel_performance.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.channel_performance")
channel_count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.channel_performance").count()
print(f"Built channel_performance with {channel_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Daily KPIs

# COMMAND ----------

print("Building daily_kpis...")

daily_kpis = fact_transactions.join(
    dim_date.select("date_key", "full_date", "year", "month", "day_of_week", "day_name", "is_weekend"),
    "date_key",
    "left"
).groupBy("date_key", "full_date", "year", "month", "day_of_week", "day_name", "is_weekend").agg(
    F.count("transaction_key").alias("transaction_count"),
    F.sum("amount_usd").cast("decimal(18,2)").alias("total_revenue"),
    F.avg("amount_usd").cast("decimal(18,2)").alias("avg_order_value"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.countDistinct("merchant_key").alias("unique_merchants"),
    F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count")
).withColumn(
    "revenue_per_customer",
    F.when(F.col("unique_customers") > 0,
           (F.col("total_revenue") / F.col("unique_customers")).cast("decimal(18,2)"))
     .otherwise(F.lit(0.0).cast("decimal(18,2)"))
).withColumn("_created_timestamp", F.current_timestamp())

daily_kpis.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.daily_kpis")
daily_kpi_count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.daily_kpis").count()
print(f"Built daily_kpis with {daily_kpi_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Monthly KPIs

# COMMAND ----------

print("Building monthly_kpis...")

monthly_kpis = fact_transactions.join(
    dim_date.select("date_key", "year", "month", "month_name"),
    "date_key",
    "left"
).groupBy("year", "month", "month_name").agg(
    F.count("transaction_key").alias("transaction_count"),
    F.sum("amount_usd").cast("decimal(18,2)").alias("total_revenue"),
    F.avg("amount_usd").cast("decimal(18,2)").alias("avg_order_value"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.countDistinct("merchant_key").alias("active_merchants"),
    F.sum(F.when(F.col("is_fraud") == True, 1).otherwise(0)).alias("fraud_count"),
    F.countDistinct("date_key").alias("active_days")
).withColumn(
    "revenue_per_customer",
    F.when(F.col("unique_customers") > 0,
           (F.col("total_revenue") / F.col("unique_customers")).cast("decimal(18,2)"))
     .otherwise(F.lit(0.0).cast("decimal(18,2)"))
).withColumn(
    "avg_daily_revenue",
    F.when(F.col("active_days") > 0,
           (F.col("total_revenue") / F.col("active_days")).cast("decimal(18,2)"))
     .otherwise(F.lit(0.0).cast("decimal(18,2)"))
).withColumn(
    "fraud_rate",
    F.when(F.col("transaction_count") > 0,
           (F.col("fraud_count") / F.col("transaction_count")).cast("decimal(5,4)"))
     .otherwise(F.lit(0.0).cast("decimal(5,4)"))
).withColumn("_created_timestamp", F.current_timestamp())

monthly_kpis.write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.monthly_kpis")
monthly_kpi_count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.monthly_kpis").count()
print(f"Built monthly_kpis with {monthly_kpi_count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

print("\n" + "="*50)
print("GOLD LAYER SUMMARY")
print("="*50)

gold_tables = [
    ("revenue_summary", f"{CATALOG}.{GOLD_SCHEMA}.revenue_summary"),
    ("customer_analytics", f"{CATALOG}.{GOLD_SCHEMA}.customer_analytics"),
    ("merchant_analytics", f"{CATALOG}.{GOLD_SCHEMA}.merchant_analytics"),
    ("channel_performance", f"{CATALOG}.{GOLD_SCHEMA}.channel_performance"),
    ("daily_kpis", f"{CATALOG}.{GOLD_SCHEMA}.daily_kpis"),
    ("monthly_kpis", f"{CATALOG}.{GOLD_SCHEMA}.monthly_kpis"),
]

total_records = 0
for label, table_name in gold_tables:
    try:
        count = spark.table(table_name).count()
        total_records += count
        print(f"  {label}: {count:,} records")
    except Exception as e:
        print(f"  {label}: Error - {e}")

print(f"\nTotal Gold Records: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# Customer segment distribution
print("Customer Segment Distribution:")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.customer_analytics")
    .groupBy("customer_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("lifetime_value_usd"), 2).alias("avg_lifetime_value"),
        F.round(F.avg("total_transactions"), 0).alias("avg_transactions"),
    )
    .orderBy(F.desc("avg_lifetime_value"))
)

# COMMAND ----------

# Channel revenue breakdown
print("Channel Revenue Breakdown:")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.channel_performance")
    .groupBy("channel")
    .agg(
        F.sum("total_revenue_usd").alias("total_revenue"),
        F.sum("transaction_count").alias("total_transactions"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------

# Monthly trend
print("Monthly Revenue Trend:")
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.monthly_kpis")
    .orderBy("year", "month")
    .select("year", "month_name", "total_revenue", "transaction_count", "unique_customers")
)

# COMMAND ----------

print("Gold layer analytics completed successfully!")
