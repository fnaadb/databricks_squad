"""
I/O utilities for reading and writing data.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.common.config import Config
from src.common.logging_utils import get_logger, LogContext
from src.common.constants import INGESTION_METADATA_COLUMNS, DELTA_PROPERTIES

logger = get_logger("common.io")


def read_csv(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    header: bool = True,
    infer_schema: bool = True,
    **options,
) -> DataFrame:
    """
    Read CSV file(s) into DataFrame.
    
    Args:
        spark: SparkSession
        path: Path to CSV file or directory
        schema: Optional schema to enforce
        header: Whether CSV has header row
        infer_schema: Whether to infer schema (ignored if schema provided)
        **options: Additional CSV reader options
    
    Returns:
        DataFrame with CSV data
    """
    reader = spark.read.format("csv").option("header", header)
    
    if schema:
        reader = reader.schema(schema)
    elif infer_schema:
        reader = reader.option("inferSchema", True)
    
    for key, value in options.items():
        reader = reader.option(key, value)
    
    return reader.load(path)


def read_json(
    spark: SparkSession,
    path: str,
    schema: Optional[StructType] = None,
    multiline: bool = False,
    **options,
) -> DataFrame:
    """
    Read JSON file(s) into DataFrame.
    
    Args:
        spark: SparkSession
        path: Path to JSON file or directory
        schema: Optional schema to enforce
        multiline: Whether JSON spans multiple lines
        **options: Additional JSON reader options
    
    Returns:
        DataFrame with JSON data
    """
    reader = spark.read.format("json").option("multiLine", multiline)
    
    if schema:
        reader = reader.schema(schema)
    
    for key, value in options.items():
        reader = reader.option(key, value)
    
    return reader.load(path)


def read_parquet(
    spark: SparkSession,
    path: str,
    **options,
) -> DataFrame:
    """
    Read Parquet file(s) into DataFrame.
    
    Args:
        spark: SparkSession
        path: Path to Parquet file or directory
        **options: Additional Parquet reader options
    
    Returns:
        DataFrame with Parquet data
    """
    reader = spark.read.format("parquet")
    
    for key, value in options.items():
        reader = reader.option(key, value)
    
    return reader.load(path)


def read_delta(
    spark: SparkSession,
    path_or_table: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> DataFrame:
    """
    Read Delta table.
    
    Args:
        spark: SparkSession
        path_or_table: Path to Delta table or table name
        version: Optional version for time travel
        timestamp: Optional timestamp for time travel
    
    Returns:
        DataFrame with Delta data
    """
    reader = spark.read.format("delta")
    
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    
    # Check if it's a table name or path
    if "." in path_or_table or not path_or_table.startswith("/"):
        return reader.table(path_or_table)
    else:
        return reader.load(path_or_table)


def write_delta(
    df: DataFrame,
    path_or_table: str,
    mode: str = "append",
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = True,
    layer: str = "bronze",
    optimize_write: bool = True,
):
    """
    Write DataFrame to Delta table.
    
    Args:
        df: DataFrame to write
        path_or_table: Destination path or table name
        mode: Write mode (append, overwrite, merge)
        partition_by: Columns to partition by
        merge_schema: Whether to merge schemas
        layer: Medallion layer for table properties
        optimize_write: Whether to enable optimized writes
    """
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    
    if optimize_write:
        writer = writer.option("optimizeWrite", "true")
    
    # Check if it's a table name or path
    if "." in path_or_table or not path_or_table.startswith("/"):
        writer.saveAsTable(path_or_table)
    else:
        writer.save(path_or_table)
    
    logger.info(f"Wrote Delta table: {path_or_table}", mode=mode)


def merge_delta(
    spark: SparkSession,
    target_table: str,
    source_df: DataFrame,
    merge_key: Union[str, List[str]],
    update_columns: Optional[List[str]] = None,
    insert_columns: Optional[List[str]] = None,
):
    """
    Merge source DataFrame into Delta target table.
    
    Args:
        spark: SparkSession
        target_table: Target Delta table name
        source_df: Source DataFrame to merge
        merge_key: Column(s) to join on
        update_columns: Columns to update on match (default: all)
        insert_columns: Columns to insert on no match (default: all)
    """
    from delta.tables import DeltaTable
    
    # Handle single key or multiple keys
    if isinstance(merge_key, str):
        merge_condition = f"target.{merge_key} = source.{merge_key}"
    else:
        conditions = [f"target.{k} = source.{k}" for k in merge_key]
        merge_condition = " AND ".join(conditions)
    
    # Get target table
    target = DeltaTable.forName(spark, target_table)
    
    # Build update dict
    if update_columns:
        update_dict = {col: f"source.{col}" for col in update_columns}
    else:
        update_dict = {col: f"source.{col}" for col in source_df.columns if col not in (merge_key if isinstance(merge_key, list) else [merge_key])}
    
    # Build insert dict
    if insert_columns:
        insert_dict = {col: f"source.{col}" for col in insert_columns}
    else:
        insert_dict = {col: f"source.{col}" for col in source_df.columns}
    
    # Execute merge
    target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set=update_dict
    ).whenNotMatchedInsert(
        values=insert_dict
    ).execute()
    
    logger.info(f"Merged into Delta table: {target_table}")


def add_ingestion_metadata(
    df: DataFrame,
    source_file: str,
    source_system: str,
    batch_id: str,
) -> DataFrame:
    """
    Add standard ingestion metadata columns to DataFrame.
    
    Args:
        df: Input DataFrame
        source_file: Source file name/path
        source_system: Source system identifier
        batch_id: Batch/run identifier
    
    Returns:
        DataFrame with metadata columns added
    """
    return df.withColumns({
        "_ingestion_timestamp": F.current_timestamp(),
        "_source_file": F.lit(source_file),
        "_source_system": F.lit(source_system),
        "_batch_id": F.lit(batch_id),
        "_load_date": F.current_date(),
    })


def create_database_if_not_exists(
    spark: SparkSession,
    catalog: str,
    schema: str,
):
    """
    Create catalog and schema if they don't exist.
    
    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    logger.info(f"Ensured catalog/schema exists: {catalog}.{schema}")


def optimize_table(
    spark: SparkSession,
    table_name: str,
    z_order_columns: Optional[List[str]] = None,
):
    """
    Optimize Delta table with optional Z-ordering.
    
    Args:
        spark: SparkSession
        table_name: Full table name
        z_order_columns: Columns to Z-order by
    """
    if z_order_columns:
        z_order_str = ", ".join(z_order_columns)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_str})")
        logger.info(f"Optimized {table_name} with Z-ORDER BY {z_order_columns}")
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        logger.info(f"Optimized {table_name}")


def vacuum_table(
    spark: SparkSession,
    table_name: str,
    retention_hours: int = 168,  # 7 days
):
    """
    Vacuum Delta table to remove old files.
    
    Args:
        spark: SparkSession
        table_name: Full table name
        retention_hours: Retention period in hours
    """
    spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
    logger.info(f"Vacuumed {table_name} with {retention_hours}h retention")
