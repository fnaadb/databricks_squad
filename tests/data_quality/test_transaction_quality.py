"""
Data quality tests for transaction data.

Tests data quality rules and thresholds for transaction data
at each layer of the medallion architecture.
"""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import functions as F

from src.common.quality import DataQualityValidator, QualityReport
from src.common.constants import (
    VALID_CURRENCIES,
    COUNTRIES,
    TransactionStatus,
    TransactionType,
    Channel,
)


class TestTransactionQualityRules:
    """Test data quality rules for transactions."""
    
    @pytest.fixture
    def validator(self, spark):
        """Create quality validator."""
        return DataQualityValidator(spark)
    
    def test_transaction_id_not_null(self, validator, sample_transactions_df):
        """Test transaction_id is never null."""
        result = validator.check_nulls(
            sample_transactions_df,
            column="transaction_id",
            threshold=0.0,  # Zero tolerance
        )
        assert result.passed, "transaction_id should never be null"
    
    def test_amount_not_null(self, validator, sample_transactions_df):
        """Test amount is never null."""
        result = validator.check_nulls(
            sample_transactions_df,
            column="amount",
            threshold=0.0,
        )
        assert result.passed, "amount should never be null"
    
    def test_currency_valid_values(self, validator, sample_transactions_df):
        """Test currency contains only valid values."""
        result = validator.check_enum(
            sample_transactions_df,
            column="currency",
            valid_values=VALID_CURRENCIES,
        )
        assert result.passed, "currency should only contain valid values"
    
    def test_status_valid_values(self, validator, sample_transactions_df):
        """Test status contains only valid values."""
        valid_statuses = [s.value for s in TransactionStatus]
        result = validator.check_enum(
            sample_transactions_df,
            column="status",
            valid_values=valid_statuses,
        )
        assert result.passed, "status should only contain valid values"
    
    def test_transaction_type_valid_values(self, validator, sample_transactions_df):
        """Test transaction_type contains only valid values."""
        valid_types = [t.value for t in TransactionType]
        result = validator.check_enum(
            sample_transactions_df,
            column="transaction_type",
            valid_values=valid_types,
        )
        assert result.passed, "transaction_type should only contain valid values"
    
    def test_channel_valid_values(self, validator, sample_transactions_df):
        """Test channel contains only valid values."""
        valid_channels = [c.value for c in Channel]
        result = validator.check_enum(
            sample_transactions_df,
            column="channel",
            valid_values=valid_channels,
        )
        assert result.passed, "channel should only contain valid values"
    
    def test_amount_positive(self, spark, validator):
        """Test amount is positive (for non-refund transactions)."""
        df = spark.createDataFrame([
            {"transaction_id": "TXN001", "amount": "100.00", "transaction_type": "PURCHASE"},
            {"transaction_id": "TXN002", "amount": "50.00", "transaction_type": "PURCHASE"},
        ])
        
        df = df.withColumn("amount_num", F.col("amount").cast("double"))
        
        result = validator.check_range(
            df.filter(F.col("transaction_type") == "PURCHASE"),
            column="amount_num",
            min_value=0.01,
            max_value=float("inf"),
        )
        assert result.passed, "Purchase amounts should be positive"
    
    def test_no_duplicate_transaction_ids(self, validator, sample_transactions_df):
        """Test no duplicate transaction IDs."""
        result = validator.check_duplicates(
            sample_transactions_df,
            columns=["transaction_id"],
            threshold=0.0,  # Zero tolerance
        )
        assert result.passed, "transaction_id should be unique"


class TestTransactionQualityThresholds:
    """Test quality thresholds for production data."""
    
    @pytest.fixture
    def validator(self, spark):
        """Create quality validator."""
        return DataQualityValidator(spark)
    
    def test_null_threshold_customer_id(self, validator, sample_transactions_df):
        """Test customer_id null rate is within threshold."""
        result = validator.check_nulls(
            sample_transactions_df,
            column="customer_id",
            threshold=0.05,  # 5% tolerance
        )
        assert result.passed, f"customer_id null rate: {result.actual_value}"
    
    def test_null_threshold_country_code(self, validator, sample_transactions_df):
        """Test country_code null rate is within threshold."""
        result = validator.check_nulls(
            sample_transactions_df,
            column="country_code",
            threshold=0.1,  # 10% tolerance
        )
        assert result.passed, f"country_code null rate: {result.actual_value}"


class TestTransactionDataProfile:
    """Test data profiling for transactions."""
    
    def test_amount_distribution(self, spark, sample_transactions_df):
        """Test amount distribution statistics."""
        df = sample_transactions_df.withColumn(
            "amount_num",
            F.col("amount").cast("double")
        )
        
        stats = df.agg(
            F.min("amount_num").alias("min"),
            F.max("amount_num").alias("max"),
            F.avg("amount_num").alias("avg"),
            F.stddev("amount_num").alias("stddev"),
        ).collect()[0]
        
        # Basic sanity checks
        assert stats["min"] > 0, "Minimum amount should be positive"
        assert stats["max"] < 1000000, "Maximum amount should be reasonable"
        assert stats["avg"] > 0, "Average amount should be positive"
    
    def test_currency_distribution(self, sample_transactions_df):
        """Test currency distribution."""
        currency_counts = (
            sample_transactions_df
            .groupBy("currency")
            .count()
            .collect()
        )
        
        assert len(currency_counts) > 0, "Should have at least one currency"
        
        # USD should be present (common test currency)
        currencies = [r.currency for r in currency_counts]
        assert "USD" in currencies, "USD should be present"
    
    def test_status_distribution(self, sample_transactions_df):
        """Test status distribution."""
        status_counts = (
            sample_transactions_df
            .groupBy("status")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        
        # COMPLETED should be most common
        if len(status_counts) > 1:
            assert status_counts[0].status in ["COMPLETED", "PENDING"], \
                "Most transactions should be COMPLETED or PENDING"


class TestQualityReportGeneration:
    """Test quality report generation."""
    
    @pytest.fixture
    def validator(self, spark):
        """Create quality validator."""
        return DataQualityValidator(spark)
    
    def test_full_quality_report(self, validator, sample_transactions_df):
        """Test generation of full quality report."""
        report = QualityReport(
            table_name="transactions_raw",
            total_rows=sample_transactions_df.count(),
        )
        
        # Run checks
        report.add_check(validator.check_nulls(
            sample_transactions_df, "transaction_id", 0.0
        ))
        report.add_check(validator.check_nulls(
            sample_transactions_df, "amount", 0.0
        ))
        report.add_check(validator.check_duplicates(
            sample_transactions_df, ["transaction_id"], 0.0
        ))
        report.add_check(validator.check_enum(
            sample_transactions_df, "currency", VALID_CURRENCIES
        ))
        
        # Report should be complete
        assert report.table_name == "transactions_raw"
        assert report.total_rows > 0
        assert len(report.checks) == 4
        assert report.overall_passed is True
    
    def test_quality_report_with_failures(self, spark, validator):
        """Test quality report captures failures."""
        # Create data with quality issues
        df = spark.createDataFrame([
            {"id": "1", "currency": "USD"},
            {"id": None, "currency": "INVALID"},  # Two issues
        ])
        
        report = QualityReport(table_name="bad_data", total_rows=2)
        
        report.add_check(validator.check_nulls(df, "id", 0.0))
        report.add_check(validator.check_enum(df, "currency", VALID_CURRENCIES))
        
        assert report.overall_passed is False
        assert report.failed_count == 2
