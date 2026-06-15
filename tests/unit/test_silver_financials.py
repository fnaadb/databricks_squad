"""
Unit tests for Silver financials transformer.
"""

import pytest
from decimal import Decimal
from pyspark.sql import functions as F
from unittest.mock import patch, MagicMock

from src.silver.silver_financials import (
    SilverFinancialsTransformer,
    CURRENCY_TO_USD,
)


class TestSilverFinancialsTransformer:
    """Tests for SilverFinancialsTransformer class."""
    
    @pytest.fixture
    def transformer(self, spark, test_config):
        """Create a transformer instance with test config."""
        return SilverFinancialsTransformer(spark, test_config)
    
    def test_transformer_initialization(self, transformer):
        """Test transformer initializes correctly."""
        assert transformer.config.environment == "test"
        assert "silver" in transformer.silver_table
        assert "fact_transactions" in transformer.silver_table
    
    def test_currency_rates_defined(self):
        """Test currency conversion rates are defined."""
        assert "USD" in CURRENCY_TO_USD
        assert CURRENCY_TO_USD["USD"] == 1.0
        assert "EUR" in CURRENCY_TO_USD
        assert "GBP" in CURRENCY_TO_USD
        # Rates should be positive
        for currency, rate in CURRENCY_TO_USD.items():
            assert rate > 0, f"Rate for {currency} should be positive"
    
    def test_validate_and_filter_valid(self, spark, transformer, sample_transactions_df):
        """Test validation passes for valid records."""
        valid_df, invalid_df = transformer._validate_and_filter(sample_transactions_df)
        
        # All sample transactions should be valid
        assert valid_df.count() == sample_transactions_df.count()
        assert invalid_df.count() == 0
    
    def test_validate_and_filter_invalid(self, spark, transformer, invalid_transactions_data):
        """Test validation catches invalid records."""
        # Create DataFrame with invalid data
        invalid_df = spark.createDataFrame(invalid_transactions_data)
        
        valid_df, rejected_df = transformer._validate_and_filter(invalid_df)
        
        # Should have rejections
        assert rejected_df.count() > 0
        # Check rejection reasons are captured
        assert "_rejection_reason" in rejected_df.columns
    
    def test_transform_types(self, transformer, sample_transactions_df):
        """Test type transformations."""
        result_df = transformer._transform_types(sample_transactions_df)
        
        # Check amount is decimal
        schema = result_df.schema
        amount_field = next(f for f in schema.fields if f.name == "amount")
        assert "decimal" in str(amount_field.dataType).lower()
        
        # Check timestamp conversion
        timestamp_field = next(f for f in schema.fields if f.name == "transaction_timestamp")
        assert "timestamp" in str(timestamp_field.dataType).lower()
    
    def test_add_surrogate_keys(self, transformer, sample_transactions_df):
        """Test surrogate key generation."""
        typed_df = transformer._transform_types(sample_transactions_df)
        result_df = transformer._add_surrogate_keys(typed_df)
        
        # Check keys are added
        columns = result_df.columns
        assert "transaction_key" in columns
        assert "customer_key" in columns
        assert "date_key" in columns
        
        # Keys should be non-null
        assert result_df.filter(F.col("transaction_key").isNull()).count() == 0
        
        # Keys should be unique per transaction
        key_count = result_df.select("transaction_key").distinct().count()
        assert key_count == result_df.count()
    
    def test_normalize_currency(self, transformer, sample_transactions_df):
        """Test currency normalization to USD."""
        typed_df = transformer._transform_types(sample_transactions_df)
        keyed_df = transformer._add_surrogate_keys(typed_df)
        result_df = transformer._normalize_currency(keyed_df)
        
        # Check USD amount column exists
        assert "amount_usd" in result_df.columns
        
        # USD records should have same amount
        usd_row = result_df.filter(F.col("currency") == "USD").first()
        assert float(usd_row.amount) == float(usd_row.amount_usd)
        
        # EUR records should be converted
        eur_row = result_df.filter(F.col("currency") == "EUR").first()
        expected_usd = float(eur_row.amount) * CURRENCY_TO_USD["EUR"]
        assert abs(float(eur_row.amount_usd) - expected_usd) < 0.01
    
    def test_add_audit_columns(self, transformer, sample_transactions_df):
        """Test audit column addition."""
        typed_df = transformer._transform_types(sample_transactions_df)
        keyed_df = transformer._add_surrogate_keys(typed_df)
        normalized_df = transformer._normalize_currency(keyed_df)
        result_df = transformer._add_audit_columns(normalized_df)
        
        # Check audit columns
        columns = result_df.columns
        assert "_created_timestamp" in columns
        assert "_updated_timestamp" in columns
        
        # Timestamps should be non-null
        assert result_df.filter(F.col("_created_timestamp").isNull()).count() == 0


class TestCurrencyNormalization:
    """Tests specifically for currency normalization logic."""
    
    @pytest.fixture
    def transformer(self, spark, test_config):
        """Create a transformer instance."""
        return SilverFinancialsTransformer(spark, test_config)
    
    def test_all_supported_currencies(self, spark, transformer):
        """Test conversion for all supported currencies."""
        # Create test data with all currencies
        data = [
            {"amount": "100.00", "currency": currency}
            for currency in CURRENCY_TO_USD.keys()
        ]
        df = spark.createDataFrame(data)
        
        result_df = transformer._normalize_currency(df)
        
        # All should have USD conversion
        assert result_df.filter(F.col("amount_usd").isNull()).count() == 0
    
    def test_unknown_currency_fallback(self, spark, transformer):
        """Test unknown currency uses fallback rate."""
        df = spark.createDataFrame([
            {"amount": "100.00", "currency": "UNKNOWN"},
        ])
        
        result_df = transformer._normalize_currency(df)
        
        # Should use fallback rate of 1.0
        row = result_df.first()
        assert float(row.amount_usd) == 100.00
    
    def test_conversion_precision(self, spark, transformer):
        """Test conversion maintains precision."""
        df = spark.createDataFrame([
            {"amount": "123.456", "currency": "EUR"},
        ])
        
        # Note: Amount will be cast to decimal(18,2) so precision is 2 decimal places
        typed_df = df.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        result_df = transformer._normalize_currency(typed_df)
        
        row = result_df.first()
        # Should round to 2 decimal places
        assert "." in str(row.amount_usd)


class TestDataQualityIntegration:
    """Tests for quality check integration in Silver transformer."""
    
    @pytest.fixture
    def transformer(self, spark, test_config):
        """Create a transformer instance."""
        return SilverFinancialsTransformer(spark, test_config)
    
    def test_quarantine_integration(self, spark, transformer):
        """Test invalid records are sent to quarantine."""
        # Create mix of valid and invalid records
        data = [
            {"transaction_id": "TXN001", "amount": "100", "currency": "USD", "status": "COMPLETED"},
            {"transaction_id": None, "amount": "50", "currency": "USD", "status": "COMPLETED"},  # Invalid
        ]
        df = spark.createDataFrame(data)
        
        valid_df, invalid_df = transformer._validate_and_filter(df)
        
        assert valid_df.count() == 1
        assert invalid_df.count() == 1
    
    def test_negative_amount_rejected(self, spark, transformer):
        """Test negative amounts are rejected."""
        df = spark.createDataFrame([
            {"transaction_id": "TXN001", "amount": "-100", "currency": "USD", "status": "COMPLETED"},
        ])
        
        valid_df, invalid_df = transformer._validate_and_filter(df)
        
        assert valid_df.count() == 0
        assert invalid_df.count() == 1
        assert "negative" in invalid_df.first()._rejection_reason.lower()
