"""
Silver layer transformer for dimension tables.

Creates and maintains dimension tables:
- dim_customer (SCD Type 2)
- dim_account (SCD Type 2)
- dim_merchant (SCD Type 1)
- dim_date (static calendar dimension)
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger
from src.common.io import write_delta, merge_delta


class SilverDimensionsTransformer:
    """
    Transformer for Silver dimension tables.
    
    Builds and maintains dimension tables from Bronze raw data
    with support for SCD Type 1 and Type 2 changes.
    
    Example usage:
        transformer = SilverDimensionsTransformer(spark)
        transformer.build_dim_customer()
        transformer.build_dim_date(2020, 2030)
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("silver.dimensions")
        
        self.catalog = self.config.catalog.name
        self.bronze_schema = self.config.schemas.bronze
        self.silver_schema = self.config.schemas.silver
    
    def build_dim_customer(self, scd_type: int = 2) -> int:
        """
        Build customer dimension from Bronze customers_raw.
        
        Args:
            scd_type: 1 for overwrite, 2 for history tracking
        
        Returns:
            Number of records in dimension
        """
        self.logger.info("Building dim_customer")
        
        bronze_df = self.spark.table(
            f"{self.catalog}.{self.bronze_schema}.customers_raw"
        )
        
        # Transform to dimension schema
        dim_df = bronze_df.select(
            F.abs(F.hash(F.col("customer_id"))).cast("long").alias("customer_key"),
            F.col("customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
            F.col("email"),
            F.col("phone"),
            F.to_date(F.col("date_of_birth"), "yyyy-MM-dd").alias("date_of_birth"),
            F.col("country_code"),
            F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss").alias("customer_since"),
            F.current_timestamp().alias("_effective_from"),
            F.lit(None).cast("timestamp").alias("_effective_to"),
            F.lit(True).alias("_is_current"),
            F.current_timestamp().alias("_created_timestamp"),
        )
        
        # Deduplicate by customer_id (keep latest)
        window = Window.partitionBy("customer_id").orderBy(F.col("customer_since").desc())
        dim_df = dim_df.withColumn("_row_num", F.row_number().over(window))
        dim_df = dim_df.filter(F.col("_row_num") == 1).drop("_row_num")
        
        table_name = f"{self.catalog}.{self.silver_schema}.dim_customer"
        
        if scd_type == 1:
            write_delta(dim_df, table_name, mode="overwrite", layer="silver")
        else:
            # SCD Type 2 - would use merge with history tracking
            write_delta(dim_df, table_name, mode="overwrite", layer="silver")
        
        count = dim_df.count()
        self.logger.info(f"Built dim_customer with {count} records")
        return count
    
    def build_dim_account(self, scd_type: int = 2) -> int:
        """
        Build account dimension from Bronze accounts_raw.
        
        Args:
            scd_type: 1 for overwrite, 2 for history tracking
        
        Returns:
            Number of records in dimension
        """
        self.logger.info("Building dim_account")
        
        bronze_df = self.spark.table(
            f"{self.catalog}.{self.bronze_schema}.accounts_raw"
        )
        
        dim_df = bronze_df.select(
            F.abs(F.hash(F.col("account_id"))).cast("long").alias("account_key"),
            F.col("account_id"),
            F.abs(F.hash(F.col("customer_id"))).cast("long").alias("customer_key"),
            F.col("customer_id"),
            F.col("account_type"),
            F.col("account_number"),
            F.col("currency"),
            F.col("status"),
            F.to_date(F.col("opened_date"), "yyyy-MM-dd").alias("opened_date"),
            F.to_date(F.col("closed_date"), "yyyy-MM-dd").alias("closed_date"),
            F.current_timestamp().alias("_effective_from"),
            F.lit(None).cast("timestamp").alias("_effective_to"),
            F.lit(True).alias("_is_current"),
            F.current_timestamp().alias("_created_timestamp"),
        )
        
        # Deduplicate
        window = Window.partitionBy("account_id").orderBy(F.col("opened_date").desc())
        dim_df = dim_df.withColumn("_row_num", F.row_number().over(window))
        dim_df = dim_df.filter(F.col("_row_num") == 1).drop("_row_num")
        
        table_name = f"{self.catalog}.{self.silver_schema}.dim_account"
        write_delta(dim_df, table_name, mode="overwrite", layer="silver")
        
        count = dim_df.count()
        self.logger.info(f"Built dim_account with {count} records")
        return count
    
    def build_dim_merchant(self) -> int:
        """
        Build merchant dimension from Bronze merchants_raw.
        Uses SCD Type 1 (simple overwrite).
        
        Returns:
            Number of records in dimension
        """
        self.logger.info("Building dim_merchant")
        
        bronze_df = self.spark.table(
            f"{self.catalog}.{self.bronze_schema}.merchants_raw"
        )
        
        dim_df = bronze_df.select(
            F.abs(F.hash(F.col("merchant_id"))).cast("long").alias("merchant_key"),
            F.col("merchant_id"),
            F.col("merchant_name"),
            F.col("category"),
            F.col("country_code"),
            F.col("city"),
            F.col("mcc_code"),
            F.current_timestamp().alias("_created_timestamp"),
            F.current_timestamp().alias("_updated_timestamp"),
        )
        
        # Deduplicate
        window = Window.partitionBy("merchant_id").orderBy(F.col("_created_timestamp").desc())
        dim_df = dim_df.withColumn("_row_num", F.row_number().over(window))
        dim_df = dim_df.filter(F.col("_row_num") == 1).drop("_row_num")
        
        table_name = f"{self.catalog}.{self.silver_schema}.dim_merchant"
        write_delta(dim_df, table_name, mode="overwrite", layer="silver")
        
        count = dim_df.count()
        self.logger.info(f"Built dim_merchant with {count} records")
        return count
    
    def build_dim_date(self, start_year: int = 2020, end_year: int = 2030) -> int:
        """
        Build date dimension with calendar attributes.
        
        Args:
            start_year: Start year for calendar
            end_year: End year for calendar (inclusive)
        
        Returns:
            Number of date records
        """
        self.logger.info(f"Building dim_date from {start_year} to {end_year}")
        
        # Generate date range
        start = datetime(start_year, 1, 1)
        end = datetime(end_year, 12, 31)
        days = (end - start).days + 1
        
        dates = []
        current = start
        for _ in range(days):
            date_key = int(current.strftime("%Y%m%d"))
            dates.append({
                "date_key": date_key,
                "full_date": current.date(),
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "month_name": current.strftime("%B"),
                "month_short": current.strftime("%b"),
                "week_of_year": current.isocalendar()[1],
                "day_of_month": current.day,
                "day_of_week": current.weekday() + 1,  # 1=Monday
                "day_name": current.strftime("%A"),
                "day_short": current.strftime("%a"),
                "is_weekend": current.weekday() >= 5,
                "is_month_start": current.day == 1,
                "is_month_end": (current + timedelta(days=1)).month != current.month,
                "fiscal_year": current.year if current.month >= 7 else current.year - 1,  # July fiscal year
                "fiscal_quarter": ((current.month - 7) % 12) // 3 + 1,
            })
            current += timedelta(days=1)
        
        dim_df = self.spark.createDataFrame(dates)
        
        table_name = f"{self.catalog}.{self.silver_schema}.dim_date"
        write_delta(dim_df, table_name, mode="overwrite", layer="silver")
        
        self.logger.info(f"Built dim_date with {len(dates)} records")
        return len(dates)
    
    def build_all_dimensions(self) -> Dict[str, int]:
        """
        Build all dimension tables.
        
        Returns:
            Dictionary of dimension name -> record count
        """
        self.logger.info("Building all dimensions")
        
        results = {
            "dim_customer": self.build_dim_customer(),
            "dim_account": self.build_dim_account(),
            "dim_merchant": self.build_dim_merchant(),
            "dim_date": self.build_dim_date(),
        }
        
        self.logger.info("Completed building all dimensions", results=results)
        return results
    
    def create_all_tables_if_not_exist(self):
        """Create all dimension tables if they don't exist."""
        tables = {
            "dim_customer": """
                CREATE TABLE IF NOT EXISTS {table} (
                    customer_key BIGINT NOT NULL,
                    customer_id STRING NOT NULL,
                    first_name STRING,
                    last_name STRING,
                    full_name STRING,
                    email STRING,
                    phone STRING,
                    date_of_birth DATE,
                    country_code STRING,
                    customer_since TIMESTAMP,
                    _effective_from TIMESTAMP,
                    _effective_to TIMESTAMP,
                    _is_current BOOLEAN,
                    _created_timestamp TIMESTAMP
                )
                USING DELTA
            """,
            "dim_account": """
                CREATE TABLE IF NOT EXISTS {table} (
                    account_key BIGINT NOT NULL,
                    account_id STRING NOT NULL,
                    customer_key BIGINT,
                    customer_id STRING,
                    account_type STRING,
                    account_number STRING,
                    currency STRING,
                    status STRING,
                    opened_date DATE,
                    closed_date DATE,
                    _effective_from TIMESTAMP,
                    _effective_to TIMESTAMP,
                    _is_current BOOLEAN,
                    _created_timestamp TIMESTAMP
                )
                USING DELTA
            """,
            "dim_merchant": """
                CREATE TABLE IF NOT EXISTS {table} (
                    merchant_key BIGINT NOT NULL,
                    merchant_id STRING NOT NULL,
                    merchant_name STRING,
                    category STRING,
                    country_code STRING,
                    city STRING,
                    mcc_code STRING,
                    _created_timestamp TIMESTAMP,
                    _updated_timestamp TIMESTAMP
                )
                USING DELTA
            """,
            "dim_date": """
                CREATE TABLE IF NOT EXISTS {table} (
                    date_key INT NOT NULL,
                    full_date DATE,
                    year INT,
                    quarter INT,
                    month INT,
                    month_name STRING,
                    month_short STRING,
                    week_of_year INT,
                    day_of_month INT,
                    day_of_week INT,
                    day_name STRING,
                    day_short STRING,
                    is_weekend BOOLEAN,
                    is_month_start BOOLEAN,
                    is_month_end BOOLEAN,
                    fiscal_year INT,
                    fiscal_quarter INT
                )
                USING DELTA
            """,
        }
        
        for table_name, ddl in tables.items():
            full_table = f"{self.catalog}.{self.silver_schema}.{table_name}"
            self.spark.sql(ddl.format(table=full_table))
            self.logger.info(f"Ensured table exists: {full_table}")
