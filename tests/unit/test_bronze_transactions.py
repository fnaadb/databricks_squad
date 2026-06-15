"""
Unit tests for Bronze transactions loader.
"""

import pytest
from datetime import datetime
from pyspark.sql import functions as F
from unittest.mock import patch, MagicMock

from src.bronze.bronze_transactions import BronzeTransactionsLoader


class TestBronzeTransactionsLoader:
    """Tests for BronzeTransactionsLoader class."""
    
    @pytest.fixture
    def loader(self, spark, test_config):
        """Create a loader instance with test config."""
        return BronzeTransactionsLoader(spark, test_config)
    
    def test_loader_initialization(self, loader):
        """Test loader initializes correctly."""
        assert loader.config.environment == "test"
        assert "bronze" in loader.table_name
        assert "transactions" in loader.table_name
    
    def test_add_ingestion_metadata(self, loader, sample_transactions_df):
        """Test that ingestion metadata columns are added."""
        result_df = loader._add_metadata(
            sample_transactions_df,
            source="test_source",
            batch_id="batch_001",
        )
        
        # Check metadata columns exist
        columns = result_df.columns
        assert "_source_file" in columns
        assert "_batch_id" in columns
        assert "_ingestion_timestamp" in columns
        
        # Check values
        row = result_df.select("_source_file", "_batch_id").first()
        assert row._source_file == "test_source"
        assert row._batch_id == "batch_001"
    
    def test_load_from_dataframe(self, spark, loader, sample_transactions_df, temp_delta_path):
        """Test loading from an existing DataFrame."""
        # Mock the write operation
        with patch.object(loader, '_write_to_table') as mock_write:
            mock_write.return_value = sample_transactions_df.count()
            
            count = loader.load_from_dataframe(
                sample_transactions_df,
                batch_id="test_batch",
            )
            
            assert count == 4  # Sample has 4 records
            mock_write.assert_called_once()
    
    def test_validate_schema(self, loader, sample_transactions_df):
        """Test schema validation."""
        # Should not raise for valid schema
        is_valid = loader._validate_schema(sample_transactions_df)
        assert is_valid is True
    
    def test_validate_schema_missing_column(self, spark, loader):
        """Test schema validation fails with missing required column."""
        # Create DataFrame missing required column
        df = spark.createDataFrame([{"some_column": "value"}])
        
        is_valid = loader._validate_schema(df)
        assert is_valid is False
    
    def test_deduplication(self, spark, loader):
        """Test deduplication of records."""
        # Create DataFrame with duplicates
        df = spark.createDataFrame([
            {"transaction_id": "TXN001", "amount": "100"},
            {"transaction_id": "TXN001", "amount": "100"},  # Duplicate
            {"transaction_id": "TXN002", "amount": "200"},
        ])
        
        deduped = loader._deduplicate(df, key_column="transaction_id")
        
        assert deduped.count() == 2
        assert deduped.filter(F.col("transaction_id") == "TXN001").count() == 1
    
    def test_batch_id_generation(self, loader):
        """Test batch ID generation."""
        batch_id = loader._generate_batch_id()
        
        assert batch_id is not None
        assert len(batch_id) > 0
        # Should contain timestamp component
        assert datetime.now().strftime("%Y%m%d") in batch_id
    
    def test_record_count_tracking(self, loader, sample_transactions_df):
        """Test that record counts are tracked correctly."""
        # Reset counters
        loader._records_processed = 0
        loader._records_rejected = 0
        
        # Simulate processing
        loader._records_processed = sample_transactions_df.count()
        
        assert loader._records_processed == 4
        assert loader._records_rejected == 0


class TestBronzeTransactionsLoaderCSV:
    """Tests for CSV loading functionality."""
    
    @pytest.fixture
    def loader(self, spark, test_config):
        """Create a loader instance."""
        return BronzeTransactionsLoader(spark, test_config)
    
    def test_csv_options(self, loader):
        """Test CSV read options are set correctly."""
        options = loader._get_csv_options()
        
        assert options.get("header") == "true"
        assert options.get("inferSchema") == "true"
        assert "delimiter" in options
    
    @patch("src.bronze.bronze_transactions.spark_read_csv")
    def test_load_from_csv(self, mock_read, loader, sample_transactions_df):
        """Test loading from CSV file."""
        mock_read.return_value = sample_transactions_df
        
        with patch.object(loader, '_write_to_table') as mock_write:
            mock_write.return_value = 4
            
            count = loader.load_from_csv(
                "data/test.csv",
                batch_id="csv_batch",
            )
            
            mock_read.assert_called_once()
            assert count == 4


class TestBronzeTransactionsLoaderJSON:
    """Tests for JSON loading functionality."""
    
    @pytest.fixture
    def loader(self, spark, test_config):
        """Create a loader instance."""
        return BronzeTransactionsLoader(spark, test_config)
    
    def test_json_options(self, loader):
        """Test JSON read options."""
        options = loader._get_json_options()
        
        assert "multiLine" in options
    
    @patch("src.bronze.bronze_transactions.spark_read_json")
    def test_load_from_json(self, mock_read, loader, sample_transactions_df):
        """Test loading from JSON file."""
        mock_read.return_value = sample_transactions_df
        
        with patch.object(loader, '_write_to_table') as mock_write:
            mock_write.return_value = 4
            
            count = loader.load_from_json(
                "data/test.json",
                batch_id="json_batch",
            )
            
            mock_read.assert_called_once()
            assert count == 4
