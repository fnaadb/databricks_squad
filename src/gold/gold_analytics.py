"""
Gold layer analytics aggregations.

Creates aggregated analytics tables optimized for Power BI consumption:
- Revenue summary by time period
- Customer analytics
- Merchant analytics
- Channel performance
- Geographic analysis
"""

from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger
from src.common.io import write_delta


class GoldAnalyticsBuilder:
    """
    Builder for Gold analytics aggregation tables.
    
    Creates pre-aggregated tables optimized for Power BI dashboards
    with proper partitioning and indexing for query performance.
    
    Example usage:
        builder = GoldAnalyticsBuilder(spark)
        builder.build_all()
        
        # Or build specific tables
        builder.build_revenue_summary()
        builder.build_customer_analytics()
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("gold.analytics")
        
        self.catalog = self.config.catalog.name
        self.silver_schema = self.config.schemas.silver
        self.gold_schema = self.config.schemas.gold
        
        # Source tables
        self.fact_transactions = f"{self.catalog}.{self.silver_schema}.fact_transactions"
        self.dim_customer = f"{self.catalog}.{self.silver_schema}.dim_customer"
        self.dim_merchant = f"{self.catalog}.{self.silver_schema}.dim_merchant"
        self.dim_date = f"{self.catalog}.{self.silver_schema}.dim_date"
    
    def build_revenue_summary(self) -> int:
        """
        Build daily/monthly revenue summary table.
        
        Aggregates:
        - Transaction counts
        - Total revenue (USD)
        - Average transaction value
        - By date, channel, transaction type, country
        
        Returns:
            Number of records created
        """
        self.logger.info("Building revenue_summary")
        
        fact_df = self.spark.table(self.fact_transactions)
        date_df = self.spark.table(self.dim_date)
        
        # Join with date dimension
        joined = fact_df.join(
            date_df,
            fact_df.date_key == date_df.date_key,
            "left"
        )
        
        # Daily aggregation
        daily_summary = joined.groupBy(
            fact_df.date_key,
            date_df.full_date,
            date_df.year,
            date_df.quarter,
            date_df.month,
            date_df.month_name,
            date_df.week_of_year,
            date_df.day_of_week,
            date_df.day_name,
            date_df.is_weekend,
            fact_df.channel,
            fact_df.transaction_type,
            fact_df.status,
            fact_df.country_code,
        ).agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_usd").alias("total_revenue_usd"),
            F.avg("amount_usd").alias("avg_transaction_value"),
            F.min("amount_usd").alias("min_transaction_value"),
            F.max("amount_usd").alias("max_transaction_value"),
            F.countDistinct("customer_key").alias("unique_customers"),
            F.countDistinct("merchant_key").alias("unique_merchants"),
            F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
            F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0)).alias("refunded_count"),
        ).withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.revenue_summary"
        write_delta(daily_summary, table_name, mode="overwrite", layer="gold")
        
        count = daily_summary.count()
        self.logger.info(f"Built revenue_summary with {count} records")
        return count
    
    def build_customer_analytics(self) -> int:
        """
        Build customer analytics summary table.
        
        Aggregates per customer:
        - Lifetime value
        - Transaction frequency
        - Average transaction value
        - First/last transaction dates
        - Preferred channels
        
        Returns:
            Number of records created
        """
        self.logger.info("Building customer_analytics")
        
        fact_df = self.spark.table(self.fact_transactions)
        customer_df = self.spark.table(self.dim_customer)
        
        # Calculate customer metrics
        customer_stats = fact_df.filter(
            F.col("status") == "COMPLETED"
        ).groupBy("customer_key").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("lifetime_value_usd"),
            F.avg("amount_usd").alias("avg_transaction_value"),
            F.min("transaction_timestamp").alias("first_transaction_date"),
            F.max("transaction_timestamp").alias("last_transaction_date"),
            F.countDistinct("merchant_key").alias("unique_merchants_visited"),
            F.countDistinct(F.month("transaction_timestamp")).alias("active_months"),
        )
        
        # Calculate preferred channel
        channel_counts = fact_df.groupBy("customer_key", "channel").agg(
            F.count("*").alias("channel_count")
        )
        window = Window.partitionBy("customer_key").orderBy(F.desc("channel_count"))
        preferred_channel = channel_counts.withColumn(
            "_rank", F.row_number().over(window)
        ).filter(F.col("_rank") == 1).select(
            "customer_key",
            F.col("channel").alias("preferred_channel")
        )
        
        # Join with customer dimension
        analytics = customer_stats.join(
            preferred_channel, "customer_key", "left"
        ).join(
            customer_df.select("customer_key", "customer_id", "full_name", "country_code", "customer_since"),
            "customer_key",
            "left"
        )
        
        # Add calculated fields
        analytics = analytics.withColumn(
            "days_as_customer",
            F.datediff(F.current_date(), F.col("customer_since"))
        ).withColumn(
            "avg_transactions_per_month",
            F.col("total_transactions") / F.greatest(F.col("active_months"), F.lit(1))
        ).withColumn(
            "days_since_last_transaction",
            F.datediff(F.current_date(), F.col("last_transaction_date"))
        ).withColumn(
            "customer_segment",
            F.when(F.col("lifetime_value_usd") >= 10000, "Premium")
             .when(F.col("lifetime_value_usd") >= 5000, "Gold")
             .when(F.col("lifetime_value_usd") >= 1000, "Silver")
             .otherwise("Bronze")
        ).withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.customer_analytics"
        write_delta(analytics, table_name, mode="overwrite", layer="gold")
        
        count = analytics.count()
        self.logger.info(f"Built customer_analytics with {count} records")
        return count
    
    def build_merchant_analytics(self) -> int:
        """
        Build merchant performance analytics table.
        
        Returns:
            Number of records created
        """
        self.logger.info("Building merchant_analytics")
        
        fact_df = self.spark.table(self.fact_transactions)
        merchant_df = self.spark.table(self.dim_merchant)
        
        merchant_stats = fact_df.filter(
            F.col("status") == "COMPLETED"
        ).groupBy("merchant_key").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("total_revenue_usd"),
            F.avg("amount_usd").alias("avg_transaction_value"),
            F.countDistinct("customer_key").alias("unique_customers"),
            F.min("transaction_timestamp").alias("first_transaction_date"),
            F.max("transaction_timestamp").alias("last_transaction_date"),
        )
        
        # Join with merchant dimension
        analytics = merchant_stats.join(
            merchant_df.select("merchant_key", "merchant_id", "merchant_name", "category", "country_code", "city"),
            "merchant_key",
            "left"
        ).withColumn(
            "avg_daily_transactions",
            F.col("total_transactions") / F.greatest(
                F.datediff(F.col("last_transaction_date"), F.col("first_transaction_date")),
                F.lit(1)
            )
        ).withColumn(
            "merchant_tier",
            F.when(F.col("total_revenue_usd") >= 100000, "Enterprise")
             .when(F.col("total_revenue_usd") >= 50000, "Large")
             .when(F.col("total_revenue_usd") >= 10000, "Medium")
             .otherwise("Small")
        ).withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.merchant_analytics"
        write_delta(analytics, table_name, mode="overwrite", layer="gold")
        
        count = analytics.count()
        self.logger.info(f"Built merchant_analytics with {count} records")
        return count
    
    def build_channel_performance(self) -> int:
        """
        Build channel performance summary table.
        
        Returns:
            Number of records created
        """
        self.logger.info("Building channel_performance")
        
        fact_df = self.spark.table(self.fact_transactions)
        date_df = self.spark.table(self.dim_date)
        
        joined = fact_df.join(date_df, "date_key", "left")
        
        # Monthly channel performance
        channel_perf = joined.groupBy(
            date_df.year,
            date_df.month,
            date_df.month_name,
            fact_df.channel,
        ).agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_usd").alias("total_revenue_usd"),
            F.avg("amount_usd").alias("avg_transaction_value"),
            F.countDistinct("customer_key").alias("unique_customers"),
            F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        ).withColumn(
            "success_rate",
            F.round(F.col("completed_count") / F.col("transaction_count") * 100, 2)
        ).withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.channel_performance"
        write_delta(channel_perf, table_name, mode="overwrite", layer="gold")
        
        count = channel_perf.count()
        self.logger.info(f"Built channel_performance with {count} records")
        return count
    
    def build_geographic_summary(self) -> int:
        """
        Build geographic analytics summary table.
        
        Returns:
            Number of records created
        """
        self.logger.info("Building geographic_summary")
        
        fact_df = self.spark.table(self.fact_transactions)
        date_df = self.spark.table(self.dim_date)
        
        joined = fact_df.join(date_df, "date_key", "left")
        
        geo_summary = joined.groupBy(
            date_df.year,
            date_df.quarter,
            fact_df.country_code,
        ).agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_usd").alias("total_revenue_usd"),
            F.avg("amount_usd").alias("avg_transaction_value"),
            F.countDistinct("customer_key").alias("unique_customers"),
            F.countDistinct("merchant_key").alias("unique_merchants"),
        ).withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.geographic_summary"
        write_delta(geo_summary, table_name, mode="overwrite", layer="gold")
        
        count = geo_summary.count()
        self.logger.info(f"Built geographic_summary with {count} records")
        return count
    
    def build_all(self) -> Dict[str, int]:
        """
        Build all Gold analytics tables.
        
        Returns:
            Dictionary of table name -> record count
        """
        self.logger.info("Building all Gold analytics tables")
        
        results = {
            "revenue_summary": self.build_revenue_summary(),
            "customer_analytics": self.build_customer_analytics(),
            "merchant_analytics": self.build_merchant_analytics(),
            "channel_performance": self.build_channel_performance(),
            "geographic_summary": self.build_geographic_summary(),
        }
        
        self.logger.info("Completed building all Gold analytics tables", results=results)
        return results
