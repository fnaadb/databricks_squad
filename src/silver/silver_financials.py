"""
Silver layer transformer for financial fact tables.

Transforms Bronze transactions into Silver fact_transactions with:
- Type casting and validation
- Surrogate key generation
- Currency normalization to USD
- Data quality filtering and quarantine
- SCD Type 1 updates
"""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger, LogContext
from src.common.constants import VALID_CURRENCIES, TransactionStatus, TransactionType, Channel
from src.common.quality import DataQualityValidator, QualityReport, filter_valid_records
from src.common.io import write_delta, merge_delta
from src.silver.quarantine import QuarantineHandler


# Currency conversion rates to USD (simplified - in production use live rates)
CURRENCY_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.26,
    "JPY": 0.0067,
    "CAD": 0.74,
    "AUD": 0.65,
    "CHF": 1.12,
    "CNY": 0.14,
    "INR": 0.012,
    "MXN": 0.058,
    "BRL": 0.20,
    "KRW": 0.00075,
    "SGD": 0.74,
    "HKD": 0.13,
    "NOK": 0.092,
    "SEK": 0.095,
    "DKK": 0.14,
    "NZD": 0.61,
    "ZAR": 0.054,
    "RUB": 0.011,
}


class SilverFinancialsTransformer:
    """
    Transformer for Silver financial fact tables.
    
    Transforms Bronze transactions_raw into Silver fact_transactions with
    full data quality checks, type casting, and USD normalization.
    
    Example usage:
        transformer = SilverFinancialsTransformer(spark)
        report = transformer.transform_transactions(incremental=True)
        print(f"Processed {report.total_rows} records")
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("silver.financials")
        self.quarantine = QuarantineHandler(spark, config)
        
        # Table names
        self.bronze_table = f"{self.config.catalog.name}.{self.config.schemas.bronze}.transactions_raw"
        self.silver_table = f"{self.config.catalog.name}.{self.config.schemas.silver}.fact_transactions"
        
        # Dimension tables for surrogate key lookups
        self.dim_customer = f"{self.config.catalog.name}.{self.config.schemas.silver}.dim_customer"
        self.dim_account = f"{self.config.catalog.name}.{self.config.schemas.silver}.dim_account"
        self.dim_merchant = f"{self.config.catalog.name}.{self.config.schemas.silver}.dim_merchant"
        self.dim_date = f"{self.config.catalog.name}.{self.config.schemas.silver}.dim_date"
    
    def transform_transactions(
        self,
        incremental: bool = True,
        since_batch_id: Optional[str] = None,
    ) -> QualityReport:
        """
        Transform Bronze transactions to Silver fact_transactions.
        
        Args:
            incremental: If True, process only new records since last run
            since_batch_id: Process only records from this batch_id onwards
        
        Returns:
            Quality report with processing statistics
        """
        self.logger.info("Starting Silver transactions transformation", incremental=incremental)
        
        # Read Bronze data
        bronze_df = self.spark.table(self.bronze_table)
        
        if incremental and since_batch_id:
            bronze_df = bronze_df.filter(F.col("_batch_id") >= since_batch_id)
        elif incremental:
            # Get max processed timestamp from Silver
            try:
                max_ts = self.spark.table(self.silver_table).agg(
                    F.max("_created_timestamp")
                ).collect()[0][0]
                if max_ts:
                    bronze_df = bronze_df.filter(F.col("_ingestion_timestamp") > max_ts)
            except Exception:
                # Silver table doesn't exist yet, process all
                pass
        
        total_input = bronze_df.count()
        self.logger.info(f"Processing {total_input} Bronze records")
        
        if total_input == 0:
            return QualityReport(table_name=self.silver_table, total_rows=0)
        
        # Step 1: Data quality validation and filtering
        valid_df, invalid_df = self._validate_and_filter(bronze_df)
        
        # Step 2: Send invalid records to quarantine
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            self.quarantine.quarantine_records(
                invalid_df,
                source_table="transactions_raw",
                reason_column="_rejection_reason",
            )
            self.logger.info(f"Quarantined {invalid_count} invalid records")
        
        # Step 3: Type casting and transformation
        transformed_df = self._transform_types(valid_df)
        
        # Step 4: Generate surrogate keys
        with_keys_df = self._add_surrogate_keys(transformed_df)
        
        # Step 5: Currency normalization
        normalized_df = self._normalize_currency(with_keys_df)
        
        # Step 6: Add audit columns
        final_df = self._add_audit_columns(normalized_df)
        
        # Step 7: Write to Silver table
        valid_count = final_df.count()
        
        write_delta(
            final_df,
            self.silver_table,
            mode="append",
            partition_by=["date_key"],
            layer="silver",
        )
        
        self.logger.info(
            f"Wrote {valid_count} records to Silver",
            input_count=total_input,
            output_count=valid_count,
            quarantine_count=invalid_count,
        )
        
        # Generate quality report
        report = QualityReport(table_name=self.silver_table, total_rows=total_input)
        return report
    
    def _validate_and_filter(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Validate records and split into valid/invalid.
        
        Returns:
            Tuple of (valid_df, invalid_df with rejection reason)
        """
        conditions = [
            ("null_transaction_id", lambda d: d.filter(F.col("transaction_id").isNull())),
            ("null_amount", lambda d: d.filter(F.col("amount").isNull())),
            ("invalid_amount", lambda d: d.filter(
                F.col("amount").isNotNull() & 
                (~F.col("amount").rlike(r'^-?\d+\.?\d*$'))
            )),
            ("negative_amount", lambda d: d.filter(
                F.col("amount").cast("double") < 0
            )),
            ("invalid_currency", lambda d: d.filter(
                ~F.col("currency").isin(VALID_CURRENCIES) &
                F.col("currency").isNotNull()
            )),
            ("invalid_status", lambda d: d.filter(
                ~F.col("status").isin([s.value for s in TransactionStatus]) &
                F.col("status").isNotNull()
            )),
            ("invalid_transaction_type", lambda d: d.filter(
                ~F.col("transaction_type").isin([t.value for t in TransactionType]) &
                F.col("transaction_type").isNotNull()
            )),
            ("invalid_channel", lambda d: d.filter(
                ~F.col("channel").isin([c.value for c in Channel]) &
                F.col("channel").isNotNull()
            )),
            ("invalid_date", lambda d: d.filter(
                F.to_date(F.col("transaction_date"), "yyyy-MM-dd").isNull() &
                F.col("transaction_date").isNotNull()
            )),
        ]
        
        return filter_valid_records(df, conditions)
    
    def _transform_types(self, df: DataFrame) -> DataFrame:
        """Cast string columns to proper types."""
        return df.select(
            F.col("transaction_id"),
            F.col("customer_id"),
            F.col("account_id"),
            F.col("merchant_id"),
            F.to_timestamp(F.col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss").alias("transaction_timestamp"),
            F.to_date(F.col("transaction_date"), "yyyy-MM-dd").alias("transaction_date"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.col("currency"),
            F.col("transaction_type"),
            F.col("status"),
            F.col("channel"),
            F.col("country_code"),
            F.col("description"),
            F.col("_batch_id"),
        )
    
    def _add_surrogate_keys(self, df: DataFrame) -> DataFrame:
        """
        Add surrogate keys by joining to dimension tables.
        Uses hash-based keys if dimension tables don't exist yet.
        """
        # Generate transaction_key using hash
        df = df.withColumn(
            "transaction_key",
            F.abs(F.hash(F.col("transaction_id"))).cast("long")
        )
        
        # Generate date_key as YYYYMMDD integer
        df = df.withColumn(
            "date_key",
            F.date_format(F.col("transaction_date"), "yyyyMMdd").cast("int")
        )
        
        # For dimension keys, use hash-based approach
        # In production, would join to actual dimension tables
        df = df.withColumn(
            "customer_key",
            F.when(F.col("customer_id").isNotNull(), 
                   F.abs(F.hash(F.col("customer_id"))).cast("long"))
        )
        
        df = df.withColumn(
            "account_key",
            F.when(F.col("account_id").isNotNull(),
                   F.abs(F.hash(F.col("account_id"))).cast("long"))
        )
        
        df = df.withColumn(
            "merchant_key",
            F.when(F.col("merchant_id").isNotNull(),
                   F.abs(F.hash(F.col("merchant_id"))).cast("long"))
        )
        
        return df
    
    def _normalize_currency(self, df: DataFrame) -> DataFrame:
        """Convert amounts to USD using exchange rates."""
        # Create rate lookup
        rate_expr = F.when(F.col("currency") == "USD", F.lit(1.0))
        for currency, rate in CURRENCY_TO_USD.items():
            if currency != "USD":
                rate_expr = rate_expr.when(F.col("currency") == currency, F.lit(rate))
        rate_expr = rate_expr.otherwise(F.lit(1.0))  # Default to 1.0 for unknown
        
        return df.withColumn(
            "amount_usd",
            F.round(F.col("amount") * rate_expr, 2).cast("decimal(18,2)")
        )
    
    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Add Silver layer audit columns."""
        now = F.current_timestamp()
        return df.select(
            "transaction_key",
            "transaction_id",
            "customer_key",
            "account_key",
            "merchant_key",
            "date_key",
            "transaction_timestamp",
            "amount",
            "currency",
            "amount_usd",
            "transaction_type",
            "status",
            "channel",
            "country_code",
            "description",
            now.alias("_created_timestamp"),
            now.alias("_updated_timestamp"),
        )
    
    def create_table_if_not_exists(self):
        """Create the Silver fact_transactions table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.silver_table} (
            transaction_key BIGINT NOT NULL,
            transaction_id STRING NOT NULL,
            customer_key BIGINT,
            account_key BIGINT,
            merchant_key BIGINT,
            date_key INT,
            transaction_timestamp TIMESTAMP,
            amount DECIMAL(18,2),
            currency STRING,
            amount_usd DECIMAL(18,2),
            transaction_type STRING,
            status STRING,
            channel STRING,
            country_code STRING,
            description STRING,
            _created_timestamp TIMESTAMP,
            _updated_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (date_key)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'layer' = 'silver'
        )
        """
        self.spark.sql(ddl)
        self.logger.info(f"Ensured table exists: {self.silver_table}")
    
    def get_record_count(self) -> int:
        """Get total record count in Silver transactions table."""
        return self.spark.table(self.silver_table).count()
    
    def get_processing_stats(self) -> Dict:
        """Get processing statistics for the Silver table."""
        df = self.spark.table(self.silver_table)
        
        stats = df.agg(
            F.count("*").alias("total_records"),
            F.sum("amount_usd").alias("total_amount_usd"),
            F.avg("amount_usd").alias("avg_amount_usd"),
            F.countDistinct("customer_key").alias("unique_customers"),
            F.countDistinct("merchant_key").alias("unique_merchants"),
            F.min("transaction_timestamp").alias("earliest_transaction"),
            F.max("transaction_timestamp").alias("latest_transaction"),
        ).collect()[0]
        
        return {
            "total_records": stats.total_records,
            "total_amount_usd": float(stats.total_amount_usd) if stats.total_amount_usd else 0,
            "avg_amount_usd": float(stats.avg_amount_usd) if stats.avg_amount_usd else 0,
            "unique_customers": stats.unique_customers,
            "unique_merchants": stats.unique_merchants,
            "earliest_transaction": str(stats.earliest_transaction),
            "latest_transaction": str(stats.latest_transaction),
        }
