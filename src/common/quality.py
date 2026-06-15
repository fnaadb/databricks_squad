"""
Data quality validation utilities.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.common.constants import VALID_CURRENCIES, TransactionStatus, TransactionType, Channel
from src.common.logging_utils import get_logger

logger = get_logger("common.quality")


@dataclass
class QualityCheckResult:
    """Result of a data quality check."""
    check_name: str
    passed: bool
    actual_value: Any
    threshold: Any
    message: str
    severity: str = "error"  # error, warning, info
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_name": self.check_name,
            "passed": self.passed,
            "actual_value": self.actual_value,
            "threshold": self.threshold,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass
class QualityReport:
    """Aggregated quality report for a DataFrame."""
    table_name: str
    total_rows: int
    checks: List[QualityCheckResult] = field(default_factory=list)
    
    @property
    def passed(self) -> bool:
        """Return True if all error-level checks passed."""
        return all(c.passed for c in self.checks if c.severity == "error")
    
    @property
    def passed_count(self) -> int:
        return sum(1 for c in self.checks if c.passed)
    
    @property
    def failed_count(self) -> int:
        return sum(1 for c in self.checks if not c.passed)
    
    def add_check(self, result: QualityCheckResult):
        self.checks.append(result)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_rows": self.total_rows,
            "passed": self.passed,
            "passed_count": self.passed_count,
            "failed_count": self.failed_count,
            "checks": [c.to_dict() for c in self.checks],
        }


class DataQualityValidator:
    """
    Data quality validator with built-in checks.
    
    Example usage:
        validator = DataQualityValidator(df, "transactions")
        report = validator.run_checks([
            validator.check_not_null("transaction_id"),
            validator.check_unique("transaction_id"),
            validator.check_in_set("currency", VALID_CURRENCIES),
            validator.check_positive("amount"),
        ])
    """
    
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.total_rows = df.count()
        self.report = QualityReport(table_name=table_name, total_rows=self.total_rows)
    
    def check_not_null(
        self,
        column: str,
        threshold_pct: float = 0.0,
        severity: str = "error",
    ) -> QualityCheckResult:
        """Check that column has no (or acceptable) null values."""
        null_count = self.df.filter(F.col(column).isNull()).count()
        null_pct = (null_count / max(self.total_rows, 1)) * 100
        passed = null_pct <= threshold_pct
        
        return QualityCheckResult(
            check_name=f"not_null_{column}",
            passed=passed,
            actual_value=round(null_pct, 2),
            threshold=threshold_pct,
            message=f"Column {column} has {null_pct:.2f}% null values (threshold: {threshold_pct}%)",
            severity=severity,
        )
    
    def check_unique(
        self,
        column: str,
        threshold_pct: float = 0.0,
        severity: str = "error",
    ) -> QualityCheckResult:
        """Check that column values are unique (or within threshold)."""
        distinct_count = self.df.select(column).distinct().count()
        non_null_count = self.df.filter(F.col(column).isNotNull()).count()
        duplicate_count = non_null_count - distinct_count
        duplicate_pct = (duplicate_count / max(non_null_count, 1)) * 100
        passed = duplicate_pct <= threshold_pct
        
        return QualityCheckResult(
            check_name=f"unique_{column}",
            passed=passed,
            actual_value=round(duplicate_pct, 2),
            threshold=threshold_pct,
            message=f"Column {column} has {duplicate_pct:.2f}% duplicates (threshold: {threshold_pct}%)",
            severity=severity,
        )
    
    def check_in_set(
        self,
        column: str,
        valid_values: List[Any],
        threshold_pct: float = 0.0,
        severity: str = "error",
    ) -> QualityCheckResult:
        """Check that column values are in a valid set."""
        invalid_count = self.df.filter(
            ~F.col(column).isin(valid_values) & F.col(column).isNotNull()
        ).count()
        invalid_pct = (invalid_count / max(self.total_rows, 1)) * 100
        passed = invalid_pct <= threshold_pct
        
        return QualityCheckResult(
            check_name=f"in_set_{column}",
            passed=passed,
            actual_value=round(invalid_pct, 2),
            threshold=threshold_pct,
            message=f"Column {column} has {invalid_pct:.2f}% invalid values (threshold: {threshold_pct}%)",
            severity=severity,
        )
    
    def check_positive(
        self,
        column: str,
        allow_zero: bool = True,
        threshold_pct: float = 0.0,
        severity: str = "error",
    ) -> QualityCheckResult:
        """Check that numeric column has positive (or non-negative) values."""
        if allow_zero:
            invalid_count = self.df.filter(F.col(column) < 0).count()
            check_desc = "non-negative"
        else:
            invalid_count = self.df.filter(F.col(column) <= 0).count()
            check_desc = "positive"
        
        invalid_pct = (invalid_count / max(self.total_rows, 1)) * 100
        passed = invalid_pct <= threshold_pct
        
        return QualityCheckResult(
            check_name=f"positive_{column}",
            passed=passed,
            actual_value=round(invalid_pct, 2),
            threshold=threshold_pct,
            message=f"Column {column} has {invalid_pct:.2f}% non-{check_desc} values (threshold: {threshold_pct}%)",
            severity=severity,
        )
    
    def check_referential_integrity(
        self,
        column: str,
        reference_df: DataFrame,
        reference_column: str,
        threshold_pct: float = 0.0,
        severity: str = "error",
    ) -> QualityCheckResult:
        """Check that column values exist in reference table."""
        # Get distinct values from reference
        valid_values = reference_df.select(reference_column).distinct()
        
        # Find orphans
        orphan_count = self.df.join(
            valid_values,
            self.df[column] == valid_values[reference_column],
            "left_anti"
        ).filter(F.col(column).isNotNull()).count()
        
        orphan_pct = (orphan_count / max(self.total_rows, 1)) * 100
        passed = orphan_pct <= threshold_pct
        
        return QualityCheckResult(
            check_name=f"ref_integrity_{column}",
            passed=passed,
            actual_value=round(orphan_pct, 2),
            threshold=threshold_pct,
            message=f"Column {column} has {orphan_pct:.2f}% orphan keys (threshold: {threshold_pct}%)",
            severity=severity,
        )
    
    def check_date_format(
        self,
        column: str,
        date_format: str = "yyyy-MM-dd",
        threshold_pct: float = 0.0,
        severity: str = "error",
    ) -> QualityCheckResult:
        """Check that date column can be parsed with expected format."""
        parsed = self.df.withColumn(
            "_parsed_date",
            F.to_date(F.col(column), date_format)
        )
        invalid_count = parsed.filter(
            F.col("_parsed_date").isNull() & F.col(column).isNotNull()
        ).count()
        
        invalid_pct = (invalid_count / max(self.total_rows, 1)) * 100
        passed = invalid_pct <= threshold_pct
        
        return QualityCheckResult(
            check_name=f"date_format_{column}",
            passed=passed,
            actual_value=round(invalid_pct, 2),
            threshold=threshold_pct,
            message=f"Column {column} has {invalid_pct:.2f}% invalid date format (expected: {date_format})",
            severity=severity,
        )
    
    def run_checks(self, checks: List[QualityCheckResult]) -> QualityReport:
        """Run a list of quality checks and return aggregated report."""
        for check in checks:
            self.report.add_check(check)
            logger.info(
                f"Quality check: {check.check_name}",
                passed=check.passed,
                actual=check.actual_value,
                threshold=check.threshold,
            )
        
        return self.report


def filter_valid_records(
    df: DataFrame,
    conditions: List[Tuple[str, Callable[[DataFrame], DataFrame]]],
) -> Tuple[DataFrame, DataFrame]:
    """
    Filter DataFrame into valid and invalid records based on conditions.
    
    Args:
        df: Input DataFrame
        conditions: List of (name, filter_function) tuples that return invalid records
    
    Returns:
        Tuple of (valid_df, invalid_df)
    """
    invalid_dfs = []
    
    for name, filter_fn in conditions:
        invalid_records = filter_fn(df).withColumn("_rejection_reason", F.lit(name))
        invalid_dfs.append(invalid_records)
    
    if invalid_dfs:
        # Combine all invalid records
        invalid_df = invalid_dfs[0]
        for idf in invalid_dfs[1:]:
            invalid_df = invalid_df.unionByName(idf, allowMissingColumns=True)
        
        # Get invalid IDs
        invalid_ids = invalid_df.select("transaction_id").distinct()
        
        # Filter valid records (exclude any that appear in invalid)
        valid_df = df.join(invalid_ids, "transaction_id", "left_anti")
    else:
        valid_df = df
        invalid_df = df.filter(F.lit(False))  # Empty DataFrame
    
    return valid_df, invalid_df


def get_transaction_quality_checks(df: DataFrame) -> List[QualityCheckResult]:
    """Get standard quality checks for transaction data."""
    validator = DataQualityValidator(df, "transactions")
    
    return [
        validator.check_not_null("transaction_id"),
        validator.check_unique("transaction_id"),
        validator.check_not_null("customer_id", threshold_pct=1.0, severity="warning"),
        validator.check_not_null("amount"),
        validator.check_positive("amount", allow_zero=False),
        validator.check_in_set("currency", VALID_CURRENCIES, threshold_pct=1.0),
        validator.check_in_set("status", [s.value for s in TransactionStatus], threshold_pct=0.5),
        validator.check_in_set("transaction_type", [t.value for t in TransactionType], threshold_pct=0.5),
        validator.check_in_set("channel", [c.value for c in Channel], threshold_pct=0.5),
        validator.check_date_format("transaction_date", "yyyy-MM-dd", threshold_pct=1.0),
    ]
