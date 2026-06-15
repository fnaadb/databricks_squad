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
from datetime import datetime
import sys

# Add src to path
sys.path.insert(0, "../")

from src.common.config import load_config
from src.common.logging_utils import get_logger
from src.gold.gold_analytics import GoldAnalyticsBuilder
from src.gold.gold_kpis import GoldKPIBuilder

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load configuration
config = load_config()
logger = get_logger("gold_analytics")

print(f"Environment: {config.environment}")
print(f"Catalog: {config.catalog.name}")
print(f"Gold Schema: {config.schemas.gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Builders

# COMMAND ----------

# Initialize Gold layer builders
analytics_builder = GoldAnalyticsBuilder(spark, config)
kpi_builder = GoldKPIBuilder(spark, config)

print("Gold layer builders initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Analytics Tables

# COMMAND ----------

# Build all analytics tables
print("Building Gold analytics tables...")
analytics_results = analytics_builder.build_all()

print("\n=== Analytics Build Results ===")
for table, count in analytics_results.items():
    print(f"{table}: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build KPI Tables

# COMMAND ----------

# Build all KPI tables
print("Building Gold KPI tables...")
kpi_results = kpi_builder.build_all()

print("\n=== KPI Build Results ===")
for table, count in kpi_results.items():
    print(f"{table}: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Gold Tables

# COMMAND ----------

# Verify record counts
print("\n=== Gold Layer Summary ===")

gold_tables = [
    (f"{config.catalog.name}.{config.schemas.gold}.revenue_summary", "revenue_summary"),
    (f"{config.catalog.name}.{config.schemas.gold}.customer_analytics", "customer_analytics"),
    (f"{config.catalog.name}.{config.schemas.gold}.merchant_analytics", "merchant_analytics"),
    (f"{config.catalog.name}.{config.schemas.gold}.channel_performance", "channel_performance"),
    (f"{config.catalog.name}.{config.schemas.gold}.geographic_summary", "geographic_summary"),
    (f"{config.catalog.name}.{config.schemas.gold}.daily_kpis", "daily_kpis"),
    (f"{config.catalog.name}.{config.schemas.gold}.monthly_kpis", "monthly_kpis"),
    (f"{config.catalog.name}.{config.schemas.gold}.executive_summary", "executive_summary"),
]

for table_name, label in gold_tables:
    try:
        count = spark.table(table_name).count()
        print(f"{label}: {count:,} records")
    except Exception as e:
        print(f"{label}: Error - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue Summary Preview

# COMMAND ----------

# Top revenue by channel
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.revenue_summary")
    .groupBy("channel")
    .agg(
        F.sum("total_revenue_usd").alias("total_revenue"),
        F.sum("transaction_count").alias("total_transactions"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Analytics Preview

# COMMAND ----------

# Customer segment distribution
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.customer_analytics")
    .groupBy("customer_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.avg("lifetime_value_usd").alias("avg_lifetime_value"),
        F.avg("total_transactions").alias("avg_transactions"),
    )
    .orderBy(F.desc("avg_lifetime_value"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily KPIs Preview

# COMMAND ----------

# Recent daily KPIs
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.daily_kpis")
    .orderBy(F.desc("date_key"))
    .select(
        "full_date",
        "total_revenue",
        "transaction_count",
        "avg_order_value",
        "revenue_trend",
        "revenue_7d_avg",
    )
    .limit(14)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Summary

# COMMAND ----------

# Latest executive summary
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.executive_summary")
    .orderBy(F.desc("report_date"))
    .limit(1)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Insights

# COMMAND ----------

# Revenue by country (top 10)
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.geographic_summary")
    .groupBy("country_code")
    .agg(F.sum("total_revenue_usd").alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

# COMMAND ----------

# Channel performance trends
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.channel_performance")
    .orderBy("year", "month", "channel")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merchant Tiers

# COMMAND ----------

# Merchant tier distribution
display(
    spark.table(f"{config.catalog.name}.{config.schemas.gold}.merchant_analytics")
    .groupBy("merchant_tier")
    .agg(
        F.count("*").alias("merchant_count"),
        F.sum("total_revenue_usd").alias("total_revenue"),
        F.avg("unique_customers").alias("avg_customers_per_merchant"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------

print("Gold analytics build completed successfully!")
