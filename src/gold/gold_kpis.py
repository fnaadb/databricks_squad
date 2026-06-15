"""
Gold layer KPI tables optimized for executive dashboards.

Creates pre-computed KPIs:
- Daily/Weekly/Monthly KPI snapshots
- Year-over-year comparisons
- Rolling averages
- Trend indicators
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger
from src.common.io import write_delta


class GoldKPIBuilder:
    """
    Builder for Gold KPI tables.
    
    Creates pre-computed KPI snapshots for dashboard consumption
    with trend indicators and period-over-period comparisons.
    
    Example usage:
        builder = GoldKPIBuilder(spark)
        builder.build_daily_kpis()
        builder.build_executive_summary()
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("gold.kpis")
        
        self.catalog = self.config.catalog.name
        self.silver_schema = self.config.schemas.silver
        self.gold_schema = self.config.schemas.gold
        
        # Source tables
        self.fact_transactions = f"{self.catalog}.{self.silver_schema}.fact_transactions"
        self.dim_date = f"{self.catalog}.{self.silver_schema}.dim_date"
    
    def build_daily_kpis(self) -> int:
        """
        Build daily KPI snapshot table.
        
        KPIs include:
        - Daily revenue
        - Transaction count
        - Average order value
        - New vs returning customers
        - 7-day and 30-day rolling averages
        
        Returns:
            Number of records created
        """
        self.logger.info("Building daily_kpis")
        
        fact_df = self.spark.table(self.fact_transactions)
        date_df = self.spark.table(self.dim_date)
        
        # Daily aggregation
        daily = fact_df.filter(
            F.col("status") == "COMPLETED"
        ).groupBy("date_key").agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_usd").alias("total_revenue"),
            F.avg("amount_usd").alias("avg_order_value"),
            F.countDistinct("customer_key").alias("active_customers"),
            F.countDistinct("merchant_key").alias("active_merchants"),
        )
        
        # Join with date dimension
        with_dates = daily.join(
            date_df.select("date_key", "full_date", "year", "month", "week_of_year", "day_of_week", "is_weekend"),
            "date_key",
            "left"
        )
        
        # Add rolling averages
        window_7d = Window.orderBy("date_key").rowsBetween(-6, 0)
        window_30d = Window.orderBy("date_key").rowsBetween(-29, 0)
        
        with_rolling = with_dates.withColumn(
            "revenue_7d_avg",
            F.avg("total_revenue").over(window_7d)
        ).withColumn(
            "revenue_30d_avg",
            F.avg("total_revenue").over(window_30d)
        ).withColumn(
            "transactions_7d_avg",
            F.avg("transaction_count").over(window_7d)
        ).withColumn(
            "transactions_30d_avg",
            F.avg("transaction_count").over(window_30d)
        )
        
        # Add day-over-day change
        window_prev = Window.orderBy("date_key")
        
        with_changes = with_rolling.withColumn(
            "prev_day_revenue",
            F.lag("total_revenue", 1).over(window_prev)
        ).withColumn(
            "revenue_dod_change",
            F.when(
                F.col("prev_day_revenue").isNotNull() & (F.col("prev_day_revenue") > 0),
                F.round((F.col("total_revenue") - F.col("prev_day_revenue")) / F.col("prev_day_revenue") * 100, 2)
            )
        ).withColumn(
            "revenue_trend",
            F.when(F.col("revenue_dod_change") > 5, "up")
             .when(F.col("revenue_dod_change") < -5, "down")
             .otherwise("stable")
        ).drop("prev_day_revenue").withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.daily_kpis"
        write_delta(with_changes, table_name, mode="overwrite", layer="gold")
        
        count = with_changes.count()
        self.logger.info(f"Built daily_kpis with {count} records")
        return count
    
    def build_monthly_kpis(self) -> int:
        """
        Build monthly KPI summary table.
        
        Returns:
            Number of records created
        """
        self.logger.info("Building monthly_kpis")
        
        fact_df = self.spark.table(self.fact_transactions)
        date_df = self.spark.table(self.dim_date)
        
        joined = fact_df.filter(
            F.col("status") == "COMPLETED"
        ).join(date_df, "date_key", "left")
        
        monthly = joined.groupBy(
            date_df.year,
            date_df.month,
            date_df.month_name,
        ).agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_usd").alias("total_revenue"),
            F.avg("amount_usd").alias("avg_order_value"),
            F.countDistinct("customer_key").alias("active_customers"),
            F.countDistinct("merchant_key").alias("active_merchants"),
            F.countDistinct("date_key").alias("active_days"),
        ).withColumn(
            "revenue_per_day",
            F.round(F.col("total_revenue") / F.col("active_days"), 2)
        ).withColumn(
            "transactions_per_day",
            F.round(F.col("transaction_count") / F.col("active_days"), 2)
        )
        
        # Add month-over-month change
        window = Window.orderBy("year", "month")
        
        with_mom = monthly.withColumn(
            "prev_month_revenue",
            F.lag("total_revenue", 1).over(window)
        ).withColumn(
            "revenue_mom_change",
            F.when(
                F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") > 0),
                F.round((F.col("total_revenue") - F.col("prev_month_revenue")) / F.col("prev_month_revenue") * 100, 2)
            )
        ).withColumn(
            "prev_year_revenue",
            F.lag("total_revenue", 12).over(window)
        ).withColumn(
            "revenue_yoy_change",
            F.when(
                F.col("prev_year_revenue").isNotNull() & (F.col("prev_year_revenue") > 0),
                F.round((F.col("total_revenue") - F.col("prev_year_revenue")) / F.col("prev_year_revenue") * 100, 2)
            )
        ).drop(
            "prev_month_revenue", "prev_year_revenue"
        ).withColumn(
            "_created_timestamp", F.current_timestamp()
        )
        
        table_name = f"{self.catalog}.{self.gold_schema}.monthly_kpis"
        write_delta(with_mom, table_name, mode="overwrite", layer="gold")
        
        count = with_mom.count()
        self.logger.info(f"Built monthly_kpis with {count} records")
        return count
    
    def build_executive_summary(self) -> int:
        """
        Build executive summary KPI table.
        
        Single row with current period vs prior period comparisons.
        
        Returns:
            Number of records created
        """
        self.logger.info("Building executive_summary")
        
        fact_df = self.spark.table(self.fact_transactions)
        
        # Get date ranges
        max_date = fact_df.agg(F.max("transaction_timestamp")).collect()[0][0]
        if not max_date:
            self.logger.warning("No transactions found")
            return 0
        
        # Current period: last 30 days
        current_start = max_date - timedelta(days=30)
        prior_start = current_start - timedelta(days=30)
        
        current_df = fact_df.filter(
            (F.col("transaction_timestamp") >= current_start) &
            (F.col("status") == "COMPLETED")
        )
        
        prior_df = fact_df.filter(
            (F.col("transaction_timestamp") >= prior_start) &
            (F.col("transaction_timestamp") < current_start) &
            (F.col("status") == "COMPLETED")
        )
        
        current_stats = current_df.agg(
            F.count("*").alias("current_transactions"),
            F.sum("amount_usd").alias("current_revenue"),
            F.avg("amount_usd").alias("current_aov"),
            F.countDistinct("customer_key").alias("current_customers"),
        ).collect()[0]
        
        prior_stats = prior_df.agg(
            F.count("*").alias("prior_transactions"),
            F.sum("amount_usd").alias("prior_revenue"),
            F.avg("amount_usd").alias("prior_aov"),
            F.countDistinct("customer_key").alias("prior_customers"),
        ).collect()[0]
        
        # Calculate changes
        def calc_change(current, prior):
            if prior and prior > 0:
                return round((current - prior) / prior * 100, 2)
            return None
        
        summary_data = [{
            "report_date": datetime.now().date(),
            "period_start": current_start.date(),
            "period_end": max_date.date(),
            "total_revenue": float(current_stats.current_revenue or 0),
            "revenue_change_pct": calc_change(
                current_stats.current_revenue or 0,
                prior_stats.prior_revenue or 0
            ),
            "total_transactions": int(current_stats.current_transactions or 0),
            "transactions_change_pct": calc_change(
                current_stats.current_transactions or 0,
                prior_stats.prior_transactions or 0
            ),
            "avg_order_value": float(current_stats.current_aov or 0),
            "aov_change_pct": calc_change(
                current_stats.current_aov or 0,
                prior_stats.prior_aov or 0
            ),
            "active_customers": int(current_stats.current_customers or 0),
            "customers_change_pct": calc_change(
                current_stats.current_customers or 0,
                prior_stats.prior_customers or 0
            ),
            "_created_timestamp": datetime.now(),
        }]
        
        summary_df = self.spark.createDataFrame(summary_data)
        
        table_name = f"{self.catalog}.{self.gold_schema}.executive_summary"
        write_delta(summary_df, table_name, mode="append", layer="gold")
        
        self.logger.info("Built executive_summary")
        return 1
    
    def build_all(self) -> Dict[str, int]:
        """
        Build all KPI tables.
        
        Returns:
            Dictionary of table name -> record count
        """
        self.logger.info("Building all KPI tables")
        
        results = {
            "daily_kpis": self.build_daily_kpis(),
            "monthly_kpis": self.build_monthly_kpis(),
            "executive_summary": self.build_executive_summary(),
        }
        
        self.logger.info("Completed building all KPI tables", results=results)
        return results
