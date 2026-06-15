"""
Bronze layer loader for transactions data.

Handles raw ingestion with:
- Schema enforcement (weak typing - all strings)
- Ingestion metadata columns
- Append-only writes to Delta
- Checkpointing for streaming (if enabled)
"""

from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger, LogContext
from src.common.schema_utils import BRONZE_TRANSACTIONS_SCHEMA, enforce_schema
from src.common.io import read_csv, read_json, write_delta, add_ingestion_metadata


class BronzeTransactionsLoader:
    """
    Loader for Bronze transactions_raw table.
    
    Ingests raw transaction data with minimal transformation,
    preserving original values as strings for later type casting.
    
    Example usage:
        loader = BronzeTransactionsLoader(spark)
        loader.load_from_csv("/path/to/transactions.csv", batch_id="batch_001")
        
        # Or from DataFrame
        loader.load_from_dataframe(df, source_file="api", source_system="real_time")
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("bronze.transactions")
        
        self.table_name = f"{self.config.catalog.name}.{self.config.schemas.bronze}.transactions_raw"
    
    def load_from_csv(
        self,
        path: str,
        batch_id: str,
        source_system: str = "file_ingest",
    ) -> int:
        """
        Load transactions from CSV file(s).
        
        Args:
            path: Path to CSV file or directory
            batch_id: Unique batch identifier
            source_system: Source system name
        
        Returns:
            Number of records loaded
        """
        self.logger.info(f"Loading transactions from CSV: {path}", batch_id=batch_id)
        
        df = read_csv(
            self.spark,
            path,
            header=True,
            infer_schema=False,  # Keep as strings
        )
        
        return self.load_from_dataframe(
            df,
            source_file=path,
            source_system=source_system,
            batch_id=batch_id,
        )
    
    def load_from_json(
        self,
        path: str,
        batch_id: str,
        source_system: str = "file_ingest",
    ) -> int:
        """
        Load transactions from JSON file(s).
        
        Args:
            path: Path to JSON file or directory
            batch_id: Unique batch identifier
            source_system: Source system name
        
        Returns:
            Number of records loaded
        """
        self.logger.info(f"Loading transactions from JSON: {path}", batch_id=batch_id)
        
        df = read_json(self.spark, path)
        
        return self.load_from_dataframe(
            df,
            source_file=path,
            source_system=source_system,
            batch_id=batch_id,
        )
    
    def load_from_dataframe(
        self,
        df: DataFrame,
        source_file: str,
        source_system: str,
        batch_id: str,
    ) -> int:
        """
        Load transactions from DataFrame.
        
        Args:
            df: Source DataFrame
            source_file: Source file/system identifier
            source_system: Source system name
            batch_id: Unique batch identifier
        
        Returns:
            Number of records loaded
        """
        self.logger.info(
            "Loading transactions from DataFrame",
            source=source_file,
            batch_id=batch_id,
        )
        
        # Cast all columns to strings for bronze layer
        string_df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
        
        # Add ingestion metadata
        with_metadata = add_ingestion_metadata(
            string_df,
            source_file=source_file,
            source_system=source_system,
            batch_id=batch_id,
        )
        
        # Enforce schema (add missing columns)
        final_df = enforce_schema(with_metadata, BRONZE_TRANSACTIONS_SCHEMA, fill_missing=True)
        
        # Count before write
        record_count = final_df.count()
        
        # Write to Delta table
        write_delta(
            final_df,
            self.table_name,
            mode="append",
            partition_by=["_load_date"],
            layer="bronze",
        )
        
        self.logger.info(
            f"Loaded {record_count} transactions to Bronze",
            record_count=record_count,
            table=self.table_name,
        )
        
        return record_count
    
    def create_table_if_not_exists(self):
        """Create the Bronze transactions table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            _ingestion_timestamp TIMESTAMP,
            _source_file STRING,
            _source_system STRING,
            _batch_id STRING,
            _load_date DATE,
            transaction_id STRING,
            customer_id STRING,
            account_id STRING,
            merchant_id STRING,
            transaction_date STRING,
            transaction_timestamp STRING,
            amount STRING,
            currency STRING,
            transaction_type STRING,
            status STRING,
            channel STRING,
            country_code STRING,
            description STRING
        )
        USING DELTA
        PARTITIONED BY (_load_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'layer' = 'bronze'
        )
        """
        self.spark.sql(ddl)
        self.logger.info(f"Ensured table exists: {self.table_name}")
    
    def get_record_count(self) -> int:
        """Get total record count in Bronze transactions table."""
        return self.spark.table(self.table_name).count()
    
    def get_batch_summary(self, batch_id: str) -> dict:
        """Get summary statistics for a specific batch."""
        df = self.spark.table(self.table_name).filter(F.col("_batch_id") == batch_id)
        
        count = df.count()
        if count == 0:
            return {"batch_id": batch_id, "record_count": 0}
        
        stats = df.agg(
            F.min("_ingestion_timestamp").alias("min_ingestion_ts"),
            F.max("_ingestion_timestamp").alias("max_ingestion_ts"),
            F.countDistinct("transaction_id").alias("distinct_transactions"),
            F.countDistinct("customer_id").alias("distinct_customers"),
        ).collect()[0]
        
        return {
            "batch_id": batch_id,
            "record_count": count,
            "min_ingestion_ts": str(stats.min_ingestion_ts),
            "max_ingestion_ts": str(stats.max_ingestion_ts),
            "distinct_transactions": stats.distinct_transactions,
            "distinct_customers": stats.distinct_customers,
        }
