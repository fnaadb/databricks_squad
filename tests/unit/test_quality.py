"""
Unit tests for data quality validation.
"""

import pytest
from pyspark.sql import functions as F

from src.common.quality import (
    DataQualityValidator,
    QualityCheckResult,
    QualityReport,
    filter_valid_records,
)
from src.common.constants import VALID_CURRENCIES, TransactionStatus


class TestQualityCheckResult:
    """Tests for QualityCheckResult dataclass."""
    
    def test_passed_check(self):
        """Test a passing quality check."""
        result = QualityCheckResult(
            check_name="null_check",
            passed=True,
            actual_value=0.0,
            threshold=0.1,
            message="No null values found",
        )
        assert result.passed is True
        assert result.actual_value == 0.0
    
    def test_failed_check(self):
        """Test a failing quality check."""
        result = QualityCheckResult(
            check_name="null_check",
            passed=False,
            actual_value=0.15,
            threshold=0.1,
            message="Null rate exceeds threshold",
        )
        assert result.passed is False
        assert result.actual_value > result.threshold


class TestQualityReport:
    """Tests for QualityReport class."""
    
    def test_empty_report(self):
        """Test empty quality report."""
        report = QualityReport(table_name="test_table", total_rows=100)
        assert report.table_name == "test_table"
        assert report.total_rows == 100
        assert len(report.checks) == 0
        assert report.overall_passed is True
    
    def test_report_with_all_passed(self):
        """Test report with all passing checks."""
        report = QualityReport(table_name="test_table", total_rows=100)
        report.add_check(QualityCheckResult("check1", True, 0.01, 0.1))
        report.add_check(QualityCheckResult("check2", True, 0.005, 0.01))
        
        assert report.overall_passed is True
        assert report.passed_count == 2
        assert report.failed_count == 0
    
    def test_report_with_failures(self):
        """Test report with some failing checks."""
        report = QualityReport(table_name="test_table", total_rows=100)
        report.add_check(QualityCheckResult("check1", True, 0.01, 0.1))
        report.add_check(QualityCheckResult("check2", False, 0.05, 0.01))
        
        assert report.overall_passed is False
        assert report.passed_count == 1
        assert report.failed_count == 1


class TestDataQualityValidator:
    """Tests for DataQualityValidator class."""
    
    def test_null_check_passes(self, spark, sample_transactions_df):
        """Test null check passes when no nulls."""
        validator = DataQualityValidator(spark)
        result = validator.check_nulls(
            sample_transactions_df,
            column="transaction_id",
            threshold=0.1,
        )
        assert result.passed is True
        assert result.actual_value == 0.0
    
    def test_null_check_fails(self, spark):
        """Test null check fails when nulls exceed threshold."""
        df = spark.createDataFrame([
            {"id": "1", "value": "a"},
            {"id": None, "value": "b"},
            {"id": None, "value": "c"},
        ])
        
        validator = DataQualityValidator(spark)
        result = validator.check_nulls(df, column="id", threshold=0.1)
        
        # 2/3 = 66.7% nulls, should fail
        assert result.passed is False
        assert result.actual_value > 0.5
    
    def test_duplicate_check_passes(self, spark, sample_transactions_df):
        """Test duplicate check passes with unique IDs."""
        validator = DataQualityValidator(spark)
        result = validator.check_duplicates(
            sample_transactions_df,
            columns=["transaction_id"],
            threshold=0.01,
        )
        assert result.passed is True
    
    def test_duplicate_check_fails(self, spark):
        """Test duplicate check fails with duplicate IDs."""
        df = spark.createDataFrame([
            {"id": "1", "value": "a"},
            {"id": "1", "value": "b"},  # Duplicate
            {"id": "2", "value": "c"},
        ])
        
        validator = DataQualityValidator(spark)
        result = validator.check_duplicates(df, columns=["id"], threshold=0.1)
        
        # 1/3 duplicates = 33.3%
        assert result.passed is False
    
    def test_range_check_passes(self, spark, sample_transactions_df):
        """Test range check for valid amounts."""
        validator = DataQualityValidator(spark)
        
        # Add numeric amount column
        df = sample_transactions_df.withColumn(
            "amount_num",
            F.col("amount").cast("double")
        )
        
        result = validator.check_range(
            df,
            column="amount_num",
            min_value=0,
            max_value=10000,
        )
        assert result.passed is True
    
    def test_enum_check_passes(self, spark, sample_transactions_df):
        """Test enum check for valid currencies."""
        validator = DataQualityValidator(spark)
        result = validator.check_enum(
            sample_transactions_df,
            column="currency",
            valid_values=VALID_CURRENCIES,
        )
        assert result.passed is True
    
    def test_enum_check_fails(self, spark):
        """Test enum check fails with invalid values."""
        df = spark.createDataFrame([
            {"currency": "USD"},
            {"currency": "INVALID"},
            {"currency": "EUR"},
        ])
        
        validator = DataQualityValidator(spark)
        result = validator.check_enum(
            df,
            column="currency",
            valid_values=VALID_CURRENCIES,
        )
        assert result.passed is False


class TestFilterValidRecords:
    """Tests for filter_valid_records function."""
    
    def test_filter_null_records(self, spark):
        """Test filtering out null records."""
        df = spark.createDataFrame([
            {"id": "1", "value": "a"},
            {"id": None, "value": "b"},
            {"id": "3", "value": "c"},
        ])
        
        conditions = [
            ("null_id", lambda d: d.filter(F.col("id").isNull())),
        ]
        
        valid_df, invalid_df = filter_valid_records(df, conditions)
        
        assert valid_df.count() == 2
        assert invalid_df.count() == 1
        assert invalid_df.filter(F.col("_rejection_reason") == "null_id").count() == 1
    
    def test_filter_multiple_conditions(self, spark):
        """Test filtering with multiple conditions."""
        df = spark.createDataFrame([
            {"id": "1", "amount": "100"},
            {"id": None, "amount": "50"},
            {"id": "3", "amount": "-10"},  # Negative
            {"id": "4", "amount": "200"},
        ])
        
        conditions = [
            ("null_id", lambda d: d.filter(F.col("id").isNull())),
            ("negative_amount", lambda d: d.filter(F.col("amount").cast("double") < 0)),
        ]
        
        valid_df, invalid_df = filter_valid_records(df, conditions)
        
        assert valid_df.count() == 2  # Records 1 and 4
        assert invalid_df.count() == 2  # Records 2 (null) and 3 (negative)
    
    def test_all_valid(self, spark, sample_transactions_df):
        """Test when all records are valid."""
        conditions = [
            ("null_id", lambda d: d.filter(F.col("transaction_id").isNull())),
        ]
        
        valid_df, invalid_df = filter_valid_records(sample_transactions_df, conditions)
        
        assert valid_df.count() == sample_transactions_df.count()
        assert invalid_df.count() == 0
    
    def test_all_invalid(self, spark):
        """Test when all records are invalid."""
        df = spark.createDataFrame([
            {"id": None},
            {"id": None},
        ])
        
        conditions = [
            ("null_id", lambda d: d.filter(F.col("id").isNull())),
        ]
        
        valid_df, invalid_df = filter_valid_records(df, conditions)
        
        assert valid_df.count() == 0
        assert invalid_df.count() == 2
