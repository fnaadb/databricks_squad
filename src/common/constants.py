"""
Constants and enumerations for the medallion pipeline.
"""

from enum import Enum
from typing import List


class Layer(str, Enum):
    """Medallion layer identifiers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class TransactionStatus(str, Enum):
    """Transaction status values."""
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


class TransactionType(str, Enum):
    """Transaction type values."""
    PURCHASE = "PURCHASE"
    REFUND = "REFUND"
    TRANSFER = "TRANSFER"
    PAYMENT = "PAYMENT"
    WITHDRAWAL = "WITHDRAWAL"
    DEPOSIT = "DEPOSIT"


class Channel(str, Enum):
    """Transaction channel values."""
    ONLINE = "ONLINE"
    MOBILE = "MOBILE"
    POS = "POS"
    ATM = "ATM"
    BRANCH = "BRANCH"


class AccountType(str, Enum):
    """Account type values."""
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    CREDIT = "CREDIT"
    INVESTMENT = "INVESTMENT"


class MerchantCategory(str, Enum):
    """Merchant category codes (simplified)."""
    RETAIL = "RETAIL"
    GROCERY = "GROCERY"
    RESTAURANT = "RESTAURANT"
    GAS = "GAS"
    TRAVEL = "TRAVEL"
    ENTERTAINMENT = "ENTERTAINMENT"
    UTILITIES = "UTILITIES"
    HEALTHCARE = "HEALTHCARE"
    EDUCATION = "EDUCATION"
    OTHER = "OTHER"


# Valid currency codes (ISO 4217)
VALID_CURRENCIES: List[str] = [
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "MXN",
    "BRL", "KRW", "SGD", "HKD", "NOK", "SEK", "DKK", "NZD", "ZAR", "RUB",
]

# Countries and regions
COUNTRIES = {
    "US": {"name": "United States", "region": "North America", "currency": "USD"},
    "CA": {"name": "Canada", "region": "North America", "currency": "CAD"},
    "MX": {"name": "Mexico", "region": "North America", "currency": "MXN"},
    "GB": {"name": "United Kingdom", "region": "Europe", "currency": "GBP"},
    "DE": {"name": "Germany", "region": "Europe", "currency": "EUR"},
    "FR": {"name": "France", "region": "Europe", "currency": "EUR"},
    "IT": {"name": "Italy", "region": "Europe", "currency": "EUR"},
    "ES": {"name": "Spain", "region": "Europe", "currency": "EUR"},
    "JP": {"name": "Japan", "region": "Asia Pacific", "currency": "JPY"},
    "CN": {"name": "China", "region": "Asia Pacific", "currency": "CNY"},
    "IN": {"name": "India", "region": "Asia Pacific", "currency": "INR"},
    "AU": {"name": "Australia", "region": "Asia Pacific", "currency": "AUD"},
    "BR": {"name": "Brazil", "region": "South America", "currency": "BRL"},
}

REGIONS: List[str] = [
    "North America",
    "Europe",
    "Asia Pacific",
    "South America",
    "Africa",
    "Middle East",
]

# Ingestion metadata columns
INGESTION_METADATA_COLUMNS = [
    "_ingestion_timestamp",
    "_source_file",
    "_source_system",
    "_batch_id",
    "_load_date",
]

# Delta table properties
DELTA_PROPERTIES = {
    "bronze": {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    },
    "silver": {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.logRetentionDuration": "interval 90 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    },
    "gold": {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.logRetentionDuration": "interval 365 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    },
}

# Bad data test markers (for intentional bad records)
BAD_DATA_MARKERS = {
    "null_marker": "TEST_NULL_",
    "duplicate_marker": "TEST_DUP_",
    "invalid_currency_marker": "TEST_BADCUR_",
    "orphan_fk_marker": "TEST_ORPHAN_",
    "negative_amount_marker": "TEST_NEG_",
    "malformed_date_marker": "TEST_BADDATE_",
}
