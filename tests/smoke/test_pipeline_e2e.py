"""
Smoke tests for the complete pipeline.

End-to-end tests that verify the pipeline works correctly
from data generation through Gold layer analytics.
"""

import pytest
from datetime import datetime
from pyspark.sql import functions as F
from unittest.mock import patch, MagicMock


class TestPipelineE2E:
    """End-to-end pipeline smoke tests."""
    
    def test_synthetic_data_generation(self, spark):
        """Test synthetic data generator produces valid data."""
        from src.generators.synthetic_data_generator import (
            SyntheticDataGenerator,
            GeneratorConfig,
        )
        
        # Small scale for testing
        config = GeneratorConfig(
            num_customers=100,
            num_accounts=200,
            num_merchants=50,
            num_transactions=1000,
        )
        
        generator = SyntheticDataGenerator(spark, config)
        
        # Generate data
        customers = generator.generate_customers()
        accounts = generator.generate_accounts()
        merchants = generator.generate_merchants()
        transactions = generator.generate_transactions()
        
        # Verify counts
        assert customers.count() == 100
        assert accounts.count() == 200
        assert merchants.count() == 50
        assert transactions.count() == 1000
        
        # Verify referential integrity
        customer_ids = set(r.customer_id for r in customers.select("customer_id").collect())
        txn_customer_ids = set(r.customer_id for r in transactions.select("customer_id").collect())
        
        # Most transactions should reference valid customers
        valid_refs = len(txn_customer_ids.intersection(customer_ids))
        assert valid_refs > 0, "Should have valid customer references"
    
    def test_bronze_ingestion_flow(self, spark, sample_transactions_df, test_config):
        """Test Bronze layer ingestion."""
        from src.bronze.bronze_transactions import BronzeTransactionsLoader
        
        loader = BronzeTransactionsLoader(spark, test_config)
        
        # Add metadata
        bronze_df = loader._add_metadata(
            sample_transactions_df,
            source="smoke_test",
            batch_id="smoke_001",
        )
        
        # Verify metadata columns
        assert "_source_file" in bronze_df.columns
        assert "_batch_id" in bronze_df.columns
        assert "_ingestion_timestamp" in bronze_df.columns
        
        # Verify data integrity
        assert bronze_df.count() == sample_transactions_df.count()
    
    def test_silver_transformation_flow(self, spark, sample_transactions_df, test_config):
        """Test Silver layer transformation."""
        from src.silver.silver_financials import SilverFinancialsTransformer
        
        transformer = SilverFinancialsTransformer(spark, test_config)
        
        # Run validation
        valid_df, invalid_df = transformer._validate_and_filter(sample_transactions_df)
        
        # Transform valid records
        typed_df = transformer._transform_types(valid_df)
        keyed_df = transformer._add_surrogate_keys(typed_df)
        normalized_df = transformer._normalize_currency(keyed_df)
        final_df = transformer._add_audit_columns(normalized_df)
        
        # Verify transformation
        assert "transaction_key" in final_df.columns
        assert "amount_usd" in final_df.columns
        assert "_created_timestamp" in final_df.columns
        
        # All valid records should be transformed
        assert final_df.count() == valid_df.count()
    
    def test_gold_aggregation_flow(self, spark, sample_transactions_df, test_config):
        """Test Gold layer aggregation logic."""
        # Prepare Silver-like data
        from pyspark.sql import functions as F
        
        silver_df = sample_transactions_df.select(
            F.abs(F.hash(F.col("transaction_id"))).cast("long").alias("transaction_key"),
            "transaction_id",
            F.abs(F.hash(F.col("customer_id"))).cast("long").alias("customer_key"),
            F.abs(F.hash(F.col("merchant_id"))).cast("long").alias("merchant_key"),
            F.date_format(F.to_date("transaction_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.to_timestamp("transaction_timestamp").alias("transaction_timestamp"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            "currency",
            F.col("amount").cast("decimal(18,2)").alias("amount_usd"),
            "transaction_type",
            "status",
            "channel",
            "country_code",
        )
        
        # Aggregate by channel
        channel_summary = silver_df.groupBy("channel").agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount_usd").alias("total_revenue"),
        )
        
        assert channel_summary.count() > 0
        assert "transaction_count" in channel_summary.columns
        assert "total_revenue" in channel_summary.columns


class TestLayerConnectivity:
    """Test connectivity between layers."""
    
    def test_bronze_to_silver_data_flow(
        self,
        spark,
        sample_transactions_df,
        test_config,
    ):
        """Test data flows correctly from Bronze to Silver."""
        from src.bronze.bronze_transactions import BronzeTransactionsLoader
        from src.silver.silver_financials import SilverFinancialsTransformer
        
        # Bronze
        bronze_loader = BronzeTransactionsLoader(spark, test_config)
        bronze_df = bronze_loader._add_metadata(
            sample_transactions_df,
            source="flow_test",
            batch_id="flow_001",
        )
        
        # Silver
        silver_transformer = SilverFinancialsTransformer(spark, test_config)
        valid_df, _ = silver_transformer._validate_and_filter(bronze_df)
        
        # Data should flow through
        assert valid_df.count() > 0
        
        # Bronze metadata should be accessible
        assert bronze_df.filter(F.col("_batch_id") == "flow_001").count() > 0
    
    def test_silver_to_gold_data_flow(
        self,
        spark,
        sample_transactions_df,
        sample_customers_df,
        test_config,
    ):
        """Test data flows correctly from Silver to Gold."""
        # Create Silver-like fact and dimension tables
        fact_df = sample_transactions_df.select(
            F.abs(F.hash(F.col("transaction_id"))).cast("long").alias("transaction_key"),
            F.abs(F.hash(F.col("customer_id"))).cast("long").alias("customer_key"),
            F.col("amount").cast("decimal(18,2)").alias("amount_usd"),
            "status",
            "channel",
        )
        
        dim_customer = sample_customers_df.select(
            F.abs(F.hash(F.col("customer_id"))).cast("long").alias("customer_key"),
            "customer_id",
            F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
        )
        
        # Join for customer analytics (Gold pattern)
        customer_stats = fact_df.filter(
            F.col("status") == "COMPLETED"
        ).groupBy("customer_key").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount_usd").alias("lifetime_value"),
        ).join(
            dim_customer,
            "customer_key",
            "left",
        )
        
        # Should produce customer-level metrics
        assert customer_stats.count() > 0
        assert "lifetime_value" in customer_stats.columns


class TestErrorHandling:
    """Test error handling throughout the pipeline."""
    
    def test_empty_input_handling(self, spark, test_config):
        """Test handling of empty input data."""
        from src.bronze.bronze_transactions import BronzeTransactionsLoader
        
        # Create empty DataFrame with schema
        empty_df = spark.createDataFrame([], schema="transaction_id STRING, amount STRING")
        
        loader = BronzeTransactionsLoader(spark, test_config)
        result_df = loader._add_metadata(empty_df, source="empty", batch_id="empty_001")
        
        assert result_df.count() == 0
        # Should still have metadata columns
        assert "_source_file" in result_df.columns
    
    def test_null_handling_in_transforms(self, spark, test_config):
        """Test null values are handled in transformations."""
        from src.silver.silver_financials import SilverFinancialsTransformer
        
        # Data with null values
        df = spark.createDataFrame([
            {"transaction_id": "TXN001", "amount": "100", "currency": "USD", "customer_id": None},
        ])
        
        transformer = SilverFinancialsTransformer(spark, test_config)
        valid_df, invalid_df = transformer._validate_and_filter(df)
        
        # Should handle nulls gracefully
        total = valid_df.count() + invalid_df.count()
        assert total == 1


class TestPerformanceBaseline:
    """Baseline performance tests."""
    
    def test_transformation_time_baseline(
        self,
        spark,
        sample_transactions_df,
        test_config,
    ):
        """Test transformation completes in reasonable time."""
        import time
        from src.silver.silver_financials import SilverFinancialsTransformer
        
        transformer = SilverFinancialsTransformer(spark, test_config)
        
        start_time = time.time()
        
        valid_df, _ = transformer._validate_and_filter(sample_transactions_df)
        typed_df = transformer._transform_types(valid_df)
        keyed_df = transformer._add_surrogate_keys(typed_df)
        normalized_df = transformer._normalize_currency(keyed_df)
        final_df = transformer._add_audit_columns(normalized_df)
        
        # Force execution
        _ = final_df.count()
        
        elapsed_time = time.time() - start_time
        
        # Should complete in under 30 seconds for test data
        assert elapsed_time < 30, f"Transformation took {elapsed_time:.2f}s"
