"""
Bronze layer loader for accounts data.
"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger
from src.common.schema_utils import BRONZE_ACCOUNTS_SCHEMA, enforce_schema
from src.common.io import read_csv, read_json, write_delta, add_ingestion_metadata


class BronzeAccountsLoader:
    """
    Loader for Bronze accounts_raw table.
    
    Example usage:
        loader = BronzeAccountsLoader(spark)
        loader.load_from_dataframe(df, source_file="core_banking", source_system="cbs")
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("bronze.accounts")
        
        self.table_name = f"{self.config.catalog.name}.{self.config.schemas.bronze}.accounts_raw"
    
    def load_from_csv(
        self,
        path: str,
        batch_id: str,
        source_system: str = "file_ingest",
    ) -> int:
        """Load accounts from CSV file(s)."""
        self.logger.info(f"Loading accounts from CSV: {path}", batch_id=batch_id)
        
        df = read_csv(self.spark, path, header=True, infer_schema=False)
        return self.load_from_dataframe(df, path, source_system, batch_id)
    
    def load_from_dataframe(
        self,
        df: DataFrame,
        source_file: str,
        source_system: str,
        batch_id: str,
    ) -> int:
        """
        Load accounts from DataFrame.
        
        Args:
            df: Source DataFrame
            source_file: Source file/system identifier
            source_system: Source system name
            batch_id: Unique batch identifier
        
        Returns:
            Number of records loaded
        """
        self.logger.info(
            "Loading accounts from DataFrame",
            source=source_file,
            batch_id=batch_id,
        )
        
        # Cast all columns to strings
        string_df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
        
        # Add ingestion metadata
        with_metadata = add_ingestion_metadata(
            string_df,
            source_file=source_file,
            source_system=source_system,
            batch_id=batch_id,
        )
        
        # Enforce schema
        final_df = enforce_schema(with_metadata, BRONZE_ACCOUNTS_SCHEMA, fill_missing=True)
        
        record_count = final_df.count()
        
        # Write to Delta
        write_delta(
            final_df,
            self.table_name,
            mode="append",
            layer="bronze",
        )
        
        self.logger.info(f"Loaded {record_count} accounts to Bronze")
        return record_count
    
    def create_table_if_not_exists(self):
        """Create the Bronze accounts table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            _ingestion_timestamp TIMESTAMP,
            _source_file STRING,
            _source_system STRING,
            account_id STRING,
            customer_id STRING,
            account_type STRING,
            account_number STRING,
            balance STRING,
            currency STRING,
            status STRING,
            opened_date STRING,
            closed_date STRING
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'layer' = 'bronze'
        )
        """
        self.spark.sql(ddl)
        self.logger.info(f"Ensured table exists: {self.table_name}")
    
    def get_record_count(self) -> int:
        """Get total record count."""
        return self.spark.table(self.table_name).count()
