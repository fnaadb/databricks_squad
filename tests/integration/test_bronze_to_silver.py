"""
Integration tests for Bronze to Silver data flow.

Tests the complete transformation pipeline from Bronze raw data
to Silver curated data with quality checks.
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from unittest.mock import patch, MagicMock

from src.bronze.bronze_transactions import BronzeTransactionsLoader
from src.silver.silver_financials import SilverFinancialsTransformer
from src.silver.quarantine import QuarantineHandler


class TestBronzeToSilverPipeline:
    """Integration tests for Bronze to Silver transformation."""
    
    @pytest.fixture
    def bronze_loader(self, spark, test_config):
        """Create Bronze loader."""
        return BronzeTransactionsLoader(spark, test_config)
    
    @pytest.fixture
    def silver_transformer(self, spark, test_config):
        """Create Silver transformer."""
        return SilverFinancialsTransformer(spark, test_config)
    
    @pytest.fixture
    def quarantine_handler(self, spark, test_config):
        """Create quarantine handler."""
        return QuarantineHandler(spark, test_config)
    
    def test_end_to_end_valid_data(
        self,
        spark,
        bronze_loader,
        silver_transformer,
        sample_transactions_df,
        temp_delta_path,
    ):
        """Test complete flow with valid data."""
        # Step 1: Add Bronze metadata
        bronze_df = bronze_loader._add_metadata(
            sample_transactions_df,
            source="test_source",
            batch_id="integration_test_001",
        )
        
        # Step 2: Validate and filter
        valid_df, invalid_df = silver_transformer._validate_and_filter(bronze_df)
        
        # Step 3: Transform types
        typed_df = silver_transformer._transform_types(valid_df)
        
        # Step 4: Add surrogate keys
        keyed_df = silver_transformer._add_surrogate_keys(typed_df)
        
        # Step 5: Normalize currency
        normalized_df = silver_transformer._normalize_currency(keyed_df)
        
        # Step 6: Add audit columns
        final_df = silver_transformer._add_audit_columns(normalized_df)
        
        # Assertions
        assert invalid_df.count() == 0, "No records should be rejected"
        assert final_df.count() == sample_transactions_df.count()
        
        # Check all required columns exist
        required_columns = [
            "transaction_key",
            "transaction_id",
            "customer_key",
            "amount_usd",
            "_created_timestamp",
        ]
        for col in required_columns:
            assert col in final_df.columns, f"Missing column: {col}"
    
    def test_end_to_end_with_rejections(
        self,
        spark,
        bronze_loader,
        silver_transformer,
        quarantine_handler,
        sample_transactions_df,
        invalid_transactions_data,
    ):
        """Test complete flow with mixed valid/invalid data."""
        # Create mixed dataset
        invalid_df = spark.createDataFrame(invalid_transactions_data)
        
        # Step 1: Process Bronze
        bronze_valid_df = bronze_loader._add_metadata(
            sample_transactions_df,
            source="valid_source",
            batch_id="batch_001",
        )
        bronze_invalid_df = bronze_loader._add_metadata(
            invalid_df,
            source="invalid_source",
            batch_id="batch_002",
        )
        
        # Combine
        combined_df = bronze_valid_df.unionByName(bronze_invalid_df, allowMissingColumns=True)
        
        # Step 2: Validate
        valid_df, rejected_df = silver_transformer._validate_and_filter(combined_df)
        
        # Assertions
        assert valid_df.count() > 0, "Should have valid records"
        assert rejected_df.count() > 0, "Should have rejected records"
        
        # Verify rejection reasons are captured
        rejection_reasons = [row._rejection_reason for row in rejected_df.collect()]
        assert all(reason is not None for reason in rejection_reasons)
    
    def test_incremental_processing(
        self,
        spark,
        silver_transformer,
        sample_transactions_df,
    ):
        """Test incremental processing based on batch_id."""
        # Simulate two batches
        batch1_df = sample_transactions_df.limit(2).withColumn(
            "_batch_id",
            F.lit("batch_001")
        ).withColumn(
            "_ingestion_timestamp",
            F.lit(datetime.now() - timedelta(hours=2))
        )
        
        batch2_df = sample_transactions_df.limit(2).withColumn(
            "_batch_id",
            F.lit("batch_002")
        ).withColumn(
            "_ingestion_timestamp",
            F.lit(datetime.now())
        )
        
        # Filter for only batch_002
        filtered_df = batch1_df.union(batch2_df).filter(
            F.col("_batch_id") == "batch_002"
        )
        
        assert filtered_df.count() == 2
        assert filtered_df.select("_batch_id").distinct().collect()[0]._batch_id == "batch_002"
    
    def test_data_lineage_preservation(
        self,
        spark,
        bronze_loader,
        silver_transformer,
        sample_transactions_df,
    ):
        """Test that data lineage is preserved through transformations."""
        source_file = "s3://bucket/path/to/file.csv"
        batch_id = "lineage_test_001"
        
        # Add Bronze metadata
        bronze_df = bronze_loader._add_metadata(
            sample_transactions_df,
            source=source_file,
            batch_id=batch_id,
        )
        
        # Transform
        valid_df, _ = silver_transformer._validate_and_filter(bronze_df)
        
        # Check lineage columns preserved
        row = valid_df.first()
        assert row._source_file == source_file
        assert row._batch_id == batch_id
    
    def test_idempotent_reprocessing(
        self,
        spark,
        silver_transformer,
        sample_transactions_df,
    ):
        """Test that reprocessing the same data produces consistent results."""
        # First run
        valid_df1, invalid_df1 = silver_transformer._validate_and_filter(sample_transactions_df)
        typed_df1 = silver_transformer._transform_types(valid_df1)
        keyed_df1 = silver_transformer._add_surrogate_keys(typed_df1)
        
        # Second run
        valid_df2, invalid_df2 = silver_transformer._validate_and_filter(sample_transactions_df)
        typed_df2 = silver_transformer._transform_types(valid_df2)
        keyed_df2 = silver_transformer._add_surrogate_keys(typed_df2)
        
        # Results should be consistent
        assert valid_df1.count() == valid_df2.count()
        assert invalid_df1.count() == invalid_df2.count()
        
        # Surrogate keys should be deterministic
        keys1 = set(row.transaction_key for row in keyed_df1.select("transaction_key").collect())
        keys2 = set(row.transaction_key for row in keyed_df2.select("transaction_key").collect())
        assert keys1 == keys2


class TestDimensionJoins:
    """Test joining facts with dimensions."""
    
    @pytest.fixture
    def silver_transformer(self, spark, test_config):
        """Create Silver transformer."""
        return SilverFinancialsTransformer(spark, test_config)
    
    def test_fact_dimension_join_integrity(
        self,
        spark,
        silver_transformer,
        sample_transactions_df,
        sample_customers_df,
    ):
        """Test that fact records can join to dimensions."""
        # Create fact table with keys
        typed_df = silver_transformer._transform_types(sample_transactions_df)
        fact_df = silver_transformer._add_surrogate_keys(typed_df)
        
        # Create dimension with keys
        dim_df = sample_customers_df.withColumn(
            "customer_key",
            F.abs(F.hash(F.col("customer_id"))).cast("long")
        )
        
        # Join
        joined_df = fact_df.join(
            dim_df.select("customer_key", "full_name"),
            "customer_key",
            "left"
        )
        
        # All transactions should have customer info (for matching customers)
        matched_count = joined_df.filter(F.col("full_name").isNotNull()).count()
        assert matched_count > 0, "Should have matching customer records"


class TestQualityMetrics:
    """Test quality metrics collection during pipeline."""
    
    @pytest.fixture
    def silver_transformer(self, spark, test_config):
        """Create Silver transformer."""
        return SilverFinancialsTransformer(spark, test_config)
    
    def test_quality_report_generation(
        self,
        spark,
        silver_transformer,
        sample_transactions_df,
    ):
        """Test quality report is generated correctly."""
        # Process data
        valid_df, invalid_df = silver_transformer._validate_and_filter(sample_transactions_df)
        
        # Calculate metrics
        total = sample_transactions_df.count()
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        assert total == valid_count + invalid_count
        assert valid_count / total >= 0.8, "At least 80% should be valid"
    
    def test_rejection_reason_distribution(
        self,
        spark,
        silver_transformer,
        invalid_transactions_data,
    ):
        """Test rejection reasons are properly categorized."""
        invalid_df = spark.createDataFrame(invalid_transactions_data)
        
        _, rejected_df = silver_transformer._validate_and_filter(invalid_df)
        
        # Get rejection reason distribution
        reasons = rejected_df.groupBy("_rejection_reason").count().collect()
        
        reason_dict = {r._rejection_reason: r["count"] for r in reasons}
        
        # Should have multiple rejection categories
        assert len(reason_dict) > 0, "Should have rejection reasons"
