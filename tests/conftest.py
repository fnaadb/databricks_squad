"""
Pytest configuration and shared fixtures for the test suite.

Provides:
- SparkSession fixture
- Sample data fixtures
- Mock configuration
- Test utilities
"""

import os
import pytest
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Dict, List, Any
from unittest.mock import MagicMock, patch

# Set test environment
os.environ["ENVIRONMENT"] = "test"
os.environ["CATALOG_NAME"] = "squad_test"


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    
    Uses a local Spark instance with Delta Lake support.
    Session scope ensures Spark is only created once per test run.
    """
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("squad-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_transactions_data() -> List[Dict[str, Any]]:
    """Sample transaction records for testing."""
    base_date = datetime(2024, 1, 15, 10, 30, 0)
    
    return [
        {
            "transaction_id": "TXN001",
            "customer_id": "CUST001",
            "account_id": "ACC001",
            "merchant_id": "MERCH001",
            "transaction_timestamp": base_date.strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_date": base_date.strftime("%Y-%m-%d"),
            "amount": "100.50",
            "currency": "USD",
            "transaction_type": "PURCHASE",
            "status": "COMPLETED",
            "channel": "ONLINE",
            "country_code": "US",
            "description": "Test purchase",
        },
        {
            "transaction_id": "TXN002",
            "customer_id": "CUST002",
            "account_id": "ACC002",
            "merchant_id": "MERCH002",
            "transaction_timestamp": (base_date + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_date": base_date.strftime("%Y-%m-%d"),
            "amount": "250.00",
            "currency": "EUR",
            "transaction_type": "PURCHASE",
            "status": "COMPLETED",
            "channel": "IN_STORE",
            "country_code": "DE",
            "description": "Test purchase EUR",
        },
        {
            "transaction_id": "TXN003",
            "customer_id": "CUST001",
            "account_id": "ACC001",
            "merchant_id": "MERCH003",
            "transaction_timestamp": (base_date + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_date": base_date.strftime("%Y-%m-%d"),
            "amount": "75.25",
            "currency": "USD",
            "transaction_type": "PURCHASE",
            "status": "PENDING",
            "channel": "MOBILE",
            "country_code": "US",
            "description": "Pending purchase",
        },
        {
            "transaction_id": "TXN004",
            "customer_id": "CUST003",
            "account_id": "ACC003",
            "merchant_id": "MERCH001",
            "transaction_timestamp": (base_date + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_date": (base_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            "amount": "500.00",
            "currency": "GBP",
            "transaction_type": "TRANSFER",
            "status": "COMPLETED",
            "channel": "ONLINE",
            "country_code": "GB",
            "description": "Transfer",
        },
    ]


@pytest.fixture
def sample_transactions_df(spark, sample_transactions_data):
    """Create a DataFrame from sample transactions."""
    return spark.createDataFrame(sample_transactions_data)


@pytest.fixture
def invalid_transactions_data() -> List[Dict[str, Any]]:
    """Sample invalid transaction records for testing quality checks."""
    return [
        # Null transaction_id
        {
            "transaction_id": None,
            "customer_id": "CUST001",
            "amount": "100.00",
            "currency": "USD",
            "status": "COMPLETED",
        },
        # Negative amount
        {
            "transaction_id": "TXN_NEG",
            "customer_id": "CUST001",
            "amount": "-50.00",
            "currency": "USD",
            "status": "COMPLETED",
        },
        # Invalid currency
        {
            "transaction_id": "TXN_BADCUR",
            "customer_id": "CUST001",
            "amount": "100.00",
            "currency": "INVALID",
            "status": "COMPLETED",
        },
        # Invalid status
        {
            "transaction_id": "TXN_BADSTAT",
            "customer_id": "CUST001",
            "amount": "100.00",
            "currency": "USD",
            "status": "UNKNOWN_STATUS",
        },
        # Malformed date
        {
            "transaction_id": "TXN_BADDATE",
            "customer_id": "CUST001",
            "amount": "100.00",
            "currency": "USD",
            "status": "COMPLETED",
            "transaction_date": "not-a-date",
        },
    ]


@pytest.fixture
def sample_customers_data() -> List[Dict[str, Any]]:
    """Sample customer records for testing."""
    return [
        {
            "customer_id": "CUST001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "phone": "+1-555-0101",
            "date_of_birth": "1990-05-15",
            "country_code": "US",
            "created_at": "2023-01-01 00:00:00",
        },
        {
            "customer_id": "CUST002",
            "first_name": "Jane",
            "last_name": "Smith",
            "email": "jane.smith@example.com",
            "phone": "+1-555-0102",
            "date_of_birth": "1985-08-22",
            "country_code": "US",
            "created_at": "2023-02-15 00:00:00",
        },
        {
            "customer_id": "CUST003",
            "first_name": "Alice",
            "last_name": "Johnson",
            "email": "alice.j@example.com",
            "phone": "+44-20-1234-5678",
            "date_of_birth": "1978-12-10",
            "country_code": "GB",
            "created_at": "2023-03-20 00:00:00",
        },
    ]


@pytest.fixture
def sample_customers_df(spark, sample_customers_data):
    """Create a DataFrame from sample customers."""
    return spark.createDataFrame(sample_customers_data)


@pytest.fixture
def sample_merchants_data() -> List[Dict[str, Any]]:
    """Sample merchant records for testing."""
    return [
        {
            "merchant_id": "MERCH001",
            "merchant_name": "Tech Store",
            "category": "ELECTRONICS",
            "country_code": "US",
            "city": "San Francisco",
            "mcc_code": "5732",
        },
        {
            "merchant_id": "MERCH002",
            "merchant_name": "Supermarket Plus",
            "category": "GROCERY",
            "country_code": "DE",
            "city": "Berlin",
            "mcc_code": "5411",
        },
        {
            "merchant_id": "MERCH003",
            "merchant_name": "Fashion Hub",
            "category": "RETAIL",
            "country_code": "US",
            "city": "New York",
            "mcc_code": "5651",
        },
    ]


@pytest.fixture
def sample_merchants_df(spark, sample_merchants_data):
    """Create a DataFrame from sample merchants."""
    return spark.createDataFrame(sample_merchants_data)


@pytest.fixture
def sample_accounts_data() -> List[Dict[str, Any]]:
    """Sample account records for testing."""
    return [
        {
            "account_id": "ACC001",
            "customer_id": "CUST001",
            "account_type": "CHECKING",
            "account_number": "1234567890",
            "currency": "USD",
            "status": "ACTIVE",
            "opened_date": "2023-01-01",
            "closed_date": None,
        },
        {
            "account_id": "ACC002",
            "customer_id": "CUST002",
            "account_type": "SAVINGS",
            "account_number": "0987654321",
            "currency": "EUR",
            "status": "ACTIVE",
            "opened_date": "2023-02-15",
            "closed_date": None,
        },
        {
            "account_id": "ACC003",
            "customer_id": "CUST003",
            "account_type": "CHECKING",
            "account_number": "1122334455",
            "currency": "GBP",
            "status": "ACTIVE",
            "opened_date": "2023-03-20",
            "closed_date": None,
        },
    ]


@pytest.fixture
def sample_accounts_df(spark, sample_accounts_data):
    """Create a DataFrame from sample accounts."""
    return spark.createDataFrame(sample_accounts_data)


@pytest.fixture
def test_config():
    """Provide a test configuration object."""
    from src.common.config import Config, CatalogConfig, SchemaConfig, QualityConfig
    
    return Config(
        environment="test",
        catalog=CatalogConfig(
            name="squad_test",
            use_unity_catalog=False,
        ),
        schemas=SchemaConfig(
            bronze="bronze",
            silver="silver",
            gold="gold",
        ),
        quality=QualityConfig(
            null_threshold=0.05,
            duplicate_threshold=0.01,
            freshness_hours=24,
        ),
        debug=True,
    )


@pytest.fixture
def mock_spark_table(spark, sample_transactions_df):
    """Mock spark.table() to return sample data."""
    original_table = spark.table
    
    def mock_table(table_name):
        if "transactions" in table_name:
            return sample_transactions_df
        return original_table(table_name)
    
    with patch.object(spark, 'table', side_effect=mock_table):
        yield


@pytest.fixture
def temp_delta_path(tmp_path):
    """Provide a temporary path for Delta tables."""
    delta_path = tmp_path / "delta_tables"
    delta_path.mkdir(exist_ok=True)
    return str(delta_path)


# Utility functions for tests

def assert_dataframe_equal(df1, df2, ignore_order=True, ignore_columns=None):
    """
    Assert two DataFrames are equal.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        ignore_order: If True, ignore row order
        ignore_columns: List of columns to ignore
    """
    from chispa import assert_df_equality
    
    if ignore_columns:
        df1 = df1.drop(*ignore_columns)
        df2 = df2.drop(*ignore_columns)
    
    assert_df_equality(df1, df2, ignore_row_order=ignore_order)


def create_test_table(spark, df, table_name, temp_path):
    """Create a Delta table from DataFrame for testing."""
    path = f"{temp_path}/{table_name}"
    df.write.format("delta").mode("overwrite").save(path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{path}'")
    return path
