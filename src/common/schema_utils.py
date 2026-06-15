"""
Schema utilities for schema validation, enforcement, and evolution.
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType,
    TimestampType,
    DateType,
    BooleanType,
    ArrayType,
    MapType,
)


# Bronze layer schemas
BRONZE_TRANSACTIONS_SCHEMA = StructType([
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_source_system", StringType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_load_date", DateType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("description", StringType(), True),
])

BRONZE_CUSTOMERS_SCHEMA = StructType([
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_source_system", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

BRONZE_ACCOUNTS_SCHEMA = StructType([
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_source_system", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("account_number", StringType(), True),
    StructField("balance", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("opened_date", StringType(), True),
    StructField("closed_date", StringType(), True),
])

BRONZE_MERCHANTS_SCHEMA = StructType([
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_source_system", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("city", StringType(), True),
    StructField("mcc_code", StringType(), True),
])


# Silver layer schemas
SILVER_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_key", LongType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("customer_key", LongType(), True),
    StructField("account_key", LongType(), True),
    StructField("merchant_key", LongType(), True),
    StructField("date_key", IntegerType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("amount_usd", DecimalType(18, 2), True),
    StructField("transaction_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("_created_timestamp", TimestampType(), True),
    StructField("_updated_timestamp", TimestampType(), True),
])


def get_schema(layer: str, table: str) -> Optional[StructType]:
    """
    Get the schema for a specific layer and table.
    
    Args:
        layer: Medallion layer (bronze, silver, gold)
        table: Table name
    
    Returns:
        StructType schema or None if not found
    """
    schemas = {
        "bronze": {
            "transactions_raw": BRONZE_TRANSACTIONS_SCHEMA,
            "customers_raw": BRONZE_CUSTOMERS_SCHEMA,
            "accounts_raw": BRONZE_ACCOUNTS_SCHEMA,
            "merchants_raw": BRONZE_MERCHANTS_SCHEMA,
        },
        "silver": {
            "fact_transactions": SILVER_TRANSACTIONS_SCHEMA,
        },
    }
    
    return schemas.get(layer, {}).get(table)


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    strict: bool = False,
) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame schema against expected schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected StructType schema
        strict: If True, extra columns in df are considered errors
    
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    actual_fields = {f.name: f for f in df.schema.fields}
    expected_fields = {f.name: f for f in expected_schema.fields}
    
    # Check for missing required columns
    for name, field in expected_fields.items():
        if name not in actual_fields:
            if not field.nullable:
                errors.append(f"Missing required column: {name}")
            else:
                errors.append(f"Missing optional column: {name}")
        else:
            # Check data type compatibility
            actual_type = actual_fields[name].dataType
            expected_type = field.dataType
            
            if not _types_compatible(actual_type, expected_type):
                errors.append(
                    f"Type mismatch for column {name}: "
                    f"expected {expected_type}, got {actual_type}"
                )
    
    # Check for extra columns in strict mode
    if strict:
        extra_cols = set(actual_fields.keys()) - set(expected_fields.keys())
        for col in extra_cols:
            errors.append(f"Unexpected column: {col}")
    
    return len(errors) == 0, errors


def _types_compatible(actual: type, expected: type) -> bool:
    """Check if actual type is compatible with expected type."""
    # Same type
    if type(actual) == type(expected):
        return True
    
    # String can be cast to most types
    if isinstance(actual, StringType):
        return True
    
    # Numeric type compatibility
    numeric_types = (IntegerType, LongType, DoubleType, DecimalType)
    if isinstance(actual, numeric_types) and isinstance(expected, numeric_types):
        return True
    
    # Date/timestamp compatibility
    if isinstance(actual, (DateType, TimestampType)) and isinstance(expected, (DateType, TimestampType)):
        return True
    
    return False


def enforce_schema(
    df: DataFrame,
    schema: StructType,
    fill_missing: bool = True,
) -> DataFrame:
    """
    Enforce schema on DataFrame by casting types and handling missing columns.
    
    Args:
        df: Input DataFrame
        schema: Target schema to enforce
        fill_missing: If True, add missing columns with null values
    
    Returns:
        DataFrame with enforced schema
    """
    from pyspark.sql import functions as F
    
    result = df
    
    for field in schema.fields:
        if field.name in df.columns:
            # Cast to expected type
            result = result.withColumn(
                field.name,
                F.col(field.name).cast(field.dataType)
            )
        elif fill_missing:
            # Add missing column with null
            result = result.withColumn(
                field.name,
                F.lit(None).cast(field.dataType)
            )
    
    # Select columns in schema order
    return result.select([f.name for f in schema.fields if f.name in result.columns])


def schema_to_ddl(schema: StructType, table_name: str, layer: str = "bronze") -> str:
    """
    Convert StructType schema to Delta CREATE TABLE DDL.
    
    Args:
        schema: PySpark StructType schema
        table_name: Target table name
        layer: Medallion layer for table properties
    
    Returns:
        DDL string for creating the table
    """
    columns = []
    for field in schema.fields:
        nullable = "" if field.nullable else " NOT NULL"
        columns.append(f"    {field.name} {_spark_type_to_sql(field.dataType)}{nullable}")
    
    columns_str = ",\n".join(columns)
    
    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} (
{columns_str}
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'layer' = '{layer}'
)"""
    
    return ddl


def _spark_type_to_sql(dtype) -> str:
    """Convert Spark DataType to SQL type string."""
    type_mapping = {
        StringType: "STRING",
        IntegerType: "INT",
        LongType: "BIGINT",
        DoubleType: "DOUBLE",
        BooleanType: "BOOLEAN",
        DateType: "DATE",
        TimestampType: "TIMESTAMP",
    }
    
    if type(dtype) in type_mapping:
        return type_mapping[type(dtype)]
    elif isinstance(dtype, DecimalType):
        return f"DECIMAL({dtype.precision}, {dtype.scale})"
    else:
        return "STRING"  # Default fallback
