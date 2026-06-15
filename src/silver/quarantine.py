"""
Quarantine handler for invalid/rejected records.

Stores records that fail data quality checks for later analysis
and potential reprocessing.
"""

from datetime import datetime
from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Config, load_config
from src.common.logging_utils import get_logger
from src.common.io import write_delta


class QuarantineHandler:
    """
    Handler for quarantined (rejected) records.
    
    Records that fail data quality checks are stored in a quarantine
    table for investigation and potential reprocessing.
    
    Example usage:
        handler = QuarantineHandler(spark)
        handler.quarantine_records(invalid_df, source_table="transactions_raw")
        
        # Analyze quarantined records
        stats = handler.get_quarantine_stats()
        
        # Reprocess after fix
        handler.reprocess_quarantine(batch_id="batch_001")
    """
    
    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        self.spark = spark
        self.config = config or load_config()
        self.logger = get_logger("silver.quarantine")
        
        self.quarantine_table = f"{self.config.catalog.name}.{self.config.schemas.silver}.quarantine"
    
    def quarantine_records(
        self,
        df: DataFrame,
        source_table: str,
        reason_column: str = "_rejection_reason",
        batch_id: Optional[str] = None,
    ) -> int:
        """
        Send records to quarantine.
        
        Args:
            df: DataFrame with rejected records
            source_table: Name of the source table
            reason_column: Column containing rejection reason
            batch_id: Optional batch identifier
        
        Returns:
            Number of records quarantined
        """
        if df.count() == 0:
            return 0
        
        # Prepare quarantine record
        quarantine_df = df.select(
            F.current_timestamp().alias("quarantine_timestamp"),
            F.lit(source_table).alias("source_table"),
            F.col(reason_column).alias("rejection_reason") if reason_column in df.columns else F.lit("unknown"),
            F.lit(batch_id).alias("batch_id"),
            F.current_date().alias("quarantine_date"),
            # Store original record as JSON string
            F.to_json(F.struct([F.col(c) for c in df.columns if c != reason_column])).alias("original_record"),
        )
        
        record_count = quarantine_df.count()
        
        write_delta(
            quarantine_df,
            self.quarantine_table,
            mode="append",
            partition_by=["quarantine_date"],
            layer="silver",
        )
        
        self.logger.warning(
            f"Quarantined {record_count} records",
            source_table=source_table,
            count=record_count,
        )
        
        return record_count
    
    def get_quarantine_stats(self, days: int = 30) -> Dict:
        """
        Get statistics on quarantined records.
        
        Args:
            days: Number of days to look back
        
        Returns:
            Dictionary with quarantine statistics
        """
        try:
            df = self.spark.table(self.quarantine_table)
        except Exception:
            return {"total_records": 0, "message": "Quarantine table does not exist"}
        
        cutoff_date = datetime.now().date()
        
        # Overall stats
        overall = df.agg(
            F.count("*").alias("total_records"),
            F.countDistinct("source_table").alias("source_tables"),
            F.countDistinct("batch_id").alias("batches"),
            F.min("quarantine_timestamp").alias("earliest"),
            F.max("quarantine_timestamp").alias("latest"),
        ).collect()[0]
        
        # By reason
        by_reason = df.groupBy("rejection_reason").agg(
            F.count("*").alias("count")
        ).orderBy(F.desc("count")).collect()
        
        # By source table
        by_source = df.groupBy("source_table").agg(
            F.count("*").alias("count")
        ).orderBy(F.desc("count")).collect()
        
        return {
            "total_records": overall.total_records,
            "source_tables": overall.source_tables,
            "batches": overall.batches,
            "earliest": str(overall.earliest),
            "latest": str(overall.latest),
            "by_reason": [{"reason": r.rejection_reason, "count": r.count} for r in by_reason],
            "by_source": [{"source": s.source_table, "count": s.count} for s in by_source],
        }
    
    def get_quarantined_records(
        self,
        source_table: Optional[str] = None,
        reason: Optional[str] = None,
        batch_id: Optional[str] = None,
        limit: int = 1000,
    ) -> DataFrame:
        """
        Retrieve quarantined records with optional filters.
        
        Args:
            source_table: Filter by source table
            reason: Filter by rejection reason
            batch_id: Filter by batch ID
            limit: Maximum records to return
        
        Returns:
            DataFrame of quarantined records
        """
        df = self.spark.table(self.quarantine_table)
        
        if source_table:
            df = df.filter(F.col("source_table") == source_table)
        if reason:
            df = df.filter(F.col("rejection_reason") == reason)
        if batch_id:
            df = df.filter(F.col("batch_id") == batch_id)
        
        return df.limit(limit)
    
    def reprocess_quarantine(
        self,
        source_table: Optional[str] = None,
        batch_id: Optional[str] = None,
        delete_after_reprocess: bool = False,
    ) -> DataFrame:
        """
        Get quarantined records for reprocessing.
        
        Args:
            source_table: Filter by source table
            batch_id: Filter by batch ID
            delete_after_reprocess: Delete records after returning
        
        Returns:
            DataFrame of records to reprocess (parsed from JSON)
        """
        df = self.get_quarantined_records(source_table=source_table, batch_id=batch_id)
        
        # Parse original_record JSON back to struct
        records = df.select(
            F.from_json(F.col("original_record"), "map<string,string>").alias("record"),
            "quarantine_timestamp",
            "rejection_reason",
        ).select(
            F.col("record.*"),
            F.col("quarantine_timestamp").alias("_original_quarantine_timestamp"),
            F.col("rejection_reason").alias("_original_rejection_reason"),
        )
        
        if delete_after_reprocess:
            self.delete_quarantine_records(source_table=source_table, batch_id=batch_id)
        
        return records
    
    def delete_quarantine_records(
        self,
        source_table: Optional[str] = None,
        batch_id: Optional[str] = None,
        older_than_days: Optional[int] = None,
    ) -> int:
        """
        Delete quarantined records.
        
        Args:
            source_table: Delete only records from this source
            batch_id: Delete only records from this batch
            older_than_days: Delete records older than this many days
        
        Returns:
            Number of records deleted
        """
        from delta.tables import DeltaTable
        
        conditions = []
        
        if source_table:
            conditions.append(f"source_table = '{source_table}'")
        if batch_id:
            conditions.append(f"batch_id = '{batch_id}'")
        if older_than_days:
            conditions.append(f"quarantine_date < current_date() - interval {older_than_days} days")
        
        if not conditions:
            self.logger.warning("No delete conditions specified, aborting delete")
            return 0
        
        condition_str = " AND ".join(conditions)
        
        # Count before delete
        count_before = self.spark.table(self.quarantine_table).filter(condition_str).count()
        
        # Execute delete
        delta_table = DeltaTable.forName(self.spark, self.quarantine_table)
        delta_table.delete(condition_str)
        
        self.logger.info(f"Deleted {count_before} quarantine records", condition=condition_str)
        return count_before
    
    def create_table_if_not_exists(self):
        """Create the quarantine table if it doesn't exist."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.quarantine_table} (
            quarantine_timestamp TIMESTAMP,
            source_table STRING,
            rejection_reason STRING,
            batch_id STRING,
            quarantine_date DATE,
            original_record STRING
        )
        USING DELTA
        PARTITIONED BY (quarantine_date)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'layer' = 'silver',
            'purpose' = 'quarantine'
        )
        """
        self.spark.sql(ddl)
        self.logger.info(f"Ensured table exists: {self.quarantine_table}")
