"""
Synthetic data generator for medallion pipeline testing.

Generates 1,000,000+ transaction records with referential integrity across:
- Customers
- Accounts
- Merchants
- Transactions

Includes intentional bad data for data quality testing:
- Null values in key fields
- Duplicate transaction IDs
- Invalid currency codes
- Orphan foreign keys
- Negative amounts
- Malformed dates
"""

import hashlib
import random
import string
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
    DateType,
)

from src.common.constants import (
    VALID_CURRENCIES,
    COUNTRIES,
    TransactionStatus,
    TransactionType,
    Channel,
    AccountType,
    MerchantCategory,
    BAD_DATA_MARKERS,
)
from src.common.logging_utils import get_logger

logger = get_logger("generators.synthetic")


@dataclass
class GeneratorConfig:
    """Configuration for data generation."""
    num_customers: int = 50000
    num_accounts: int = 100000
    num_merchants: int = 10000
    num_transactions: int = 1000000
    
    # Date range for transactions
    start_date: datetime = datetime(2023, 1, 1)
    end_date: datetime = datetime(2024, 12, 31)
    
    # Bad data percentages (for testing)
    null_pct: float = 0.5
    duplicate_pct: float = 0.3
    invalid_currency_pct: float = 0.2
    orphan_fk_pct: float = 0.1
    negative_amount_pct: float = 0.1
    malformed_date_pct: float = 0.2
    
    # Random seed for reproducibility
    seed: int = 42


class SyntheticDataGenerator:
    """
    Generates synthetic data for the medallion pipeline.
    
    Example usage:
        spark = SparkSession.builder.getOrCreate()
        generator = SyntheticDataGenerator(spark, GeneratorConfig())
        
        customers_df = generator.generate_customers()
        accounts_df = generator.generate_accounts()
        merchants_df = generator.generate_merchants()
        transactions_df = generator.generate_transactions()
    """
    
    def __init__(self, spark: SparkSession, config: Optional[GeneratorConfig] = None):
        self.spark = spark
        self.config = config or GeneratorConfig()
        random.seed(self.config.seed)
        
        # Track generated IDs for referential integrity
        self._customer_ids: List[str] = []
        self._account_ids: List[str] = []
        self._merchant_ids: List[str] = []
        
        # Lookup maps
        self._customer_to_accounts: Dict[str, List[str]] = {}
        
        # First and last names for generation
        self._first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
            "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
            "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
            "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
            "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
        ]
        
        self._last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
            "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
            "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
            "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
            "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
        ]
        
        self._merchant_names = [
            "Walmart", "Amazon", "Costco", "Target", "Kroger", "Walgreens", "CVS", "Home Depot",
            "Lowe's", "Best Buy", "Starbucks", "McDonald's", "Subway", "Taco Bell", "Chipotle",
            "Uber", "Lyft", "DoorDash", "Netflix", "Spotify", "Apple", "Google", "Microsoft",
            "Shell", "Exxon", "Chevron", "BP", "United Airlines", "Delta Airlines", "Marriott",
            "Hilton", "Whole Foods", "Trader Joe's", "Safeway", "Publix", "Aldi", "Petco",
            "GameStop", "Nike", "Adidas", "Gap", "Old Navy", "H&M", "Zara", "Nordstrom", "Macy's",
        ]
    
    def _generate_id(self, prefix: str = "") -> str:
        """Generate a unique ID with optional prefix."""
        return f"{prefix}{uuid.uuid4().hex[:12].upper()}"
    
    def _generate_deterministic_id(self, seed_value: str, prefix: str = "") -> str:
        """Generate deterministic ID from seed value."""
        hash_val = hashlib.md5(seed_value.encode()).hexdigest()[:12].upper()
        return f"{prefix}{hash_val}"
    
    def _random_date(self, start: datetime, end: datetime) -> datetime:
        """Generate random datetime between start and end."""
        delta = end - start
        random_seconds = random.randint(0, int(delta.total_seconds()))
        return start + timedelta(seconds=random_seconds)
    
    def _random_choice(self, choices: List[Any]) -> Any:
        """Random choice from list."""
        return random.choice(choices)
    
    def generate_customers(self) -> DataFrame:
        """
        Generate customer master data.
        
        Returns:
            DataFrame with customer records
        """
        logger.info(f"Generating {self.config.num_customers} customers")
        
        customers = []
        for i in range(self.config.num_customers):
            customer_id = self._generate_deterministic_id(f"cust_{i}", "CUST")
            self._customer_ids.append(customer_id)
            
            first_name = self._random_choice(self._first_names)
            last_name = self._random_choice(self._last_names)
            country = self._random_choice(list(COUNTRIES.keys()))
            
            # Generate realistic email
            email_domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "company.com"])
            email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{email_domain}"
            
            # Phone
            phone = f"+1{random.randint(200, 999)}{random.randint(1000000, 9999999)}"
            
            # Dates
            dob_start = datetime(1950, 1, 1)
            dob_end = datetime(2000, 12, 31)
            dob = self._random_date(dob_start, dob_end)
            
            created_at = self._random_date(
                self.config.start_date - timedelta(days=365),
                self.config.start_date
            )
            
            customers.append({
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "date_of_birth": dob.strftime("%Y-%m-%d"),
                "country_code": country,
                "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updated_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            })
        
        df = self.spark.createDataFrame(customers)
        logger.info(f"Generated {df.count()} customers")
        return df
    
    def generate_accounts(self) -> DataFrame:
        """
        Generate account data with customer foreign keys.
        
        Returns:
            DataFrame with account records
        """
        if not self._customer_ids:
            raise ValueError("Generate customers first")
        
        logger.info(f"Generating {self.config.num_accounts} accounts")
        
        accounts = []
        accounts_per_customer = self.config.num_accounts // len(self._customer_ids) + 1
        
        for customer_id in self._customer_ids:
            num_accounts = random.randint(1, min(3, accounts_per_customer))
            customer_accounts = []
            
            for j in range(num_accounts):
                if len(accounts) >= self.config.num_accounts:
                    break
                
                account_id = self._generate_deterministic_id(f"acct_{customer_id}_{j}", "ACCT")
                customer_accounts.append(account_id)
                self._account_ids.append(account_id)
                
                account_type = self._random_choice([t.value for t in AccountType])
                country = self._random_choice(list(COUNTRIES.keys()))
                currency = COUNTRIES[country]["currency"]
                
                opened_date = self._random_date(
                    self.config.start_date - timedelta(days=730),
                    self.config.start_date
                )
                
                accounts.append({
                    "account_id": account_id,
                    "customer_id": customer_id,
                    "account_type": account_type,
                    "account_number": f"{''.join(random.choices(string.digits, k=16))}",
                    "balance": str(round(random.uniform(100, 100000), 2)),
                    "currency": currency,
                    "status": "ACTIVE",
                    "opened_date": opened_date.strftime("%Y-%m-%d"),
                    "closed_date": None,
                })
            
            self._customer_to_accounts[customer_id] = customer_accounts
        
        df = self.spark.createDataFrame(accounts)
        logger.info(f"Generated {df.count()} accounts")
        return df
    
    def generate_merchants(self) -> DataFrame:
        """
        Generate merchant master data.
        
        Returns:
            DataFrame with merchant records
        """
        logger.info(f"Generating {self.config.num_merchants} merchants")
        
        merchants = []
        for i in range(self.config.num_merchants):
            merchant_id = self._generate_deterministic_id(f"merch_{i}", "MERCH")
            self._merchant_ids.append(merchant_id)
            
            base_name = self._random_choice(self._merchant_names)
            # Add location suffix to make unique
            city = random.choice(["NYC", "LA", "CHI", "HOU", "PHX", "PHI", "SA", "SD", "DAL", "SJ"])
            merchant_name = f"{base_name} - {city} #{random.randint(1, 999)}"
            
            category = self._random_choice([c.value for c in MerchantCategory])
            country = self._random_choice(list(COUNTRIES.keys()))
            
            # MCC code (4 digits)
            mcc_mapping = {
                "RETAIL": "5411",
                "GROCERY": "5411",
                "RESTAURANT": "5812",
                "GAS": "5541",
                "TRAVEL": "4722",
                "ENTERTAINMENT": "7922",
                "UTILITIES": "4900",
                "HEALTHCARE": "8011",
                "EDUCATION": "8220",
                "OTHER": "5999",
            }
            mcc_code = mcc_mapping.get(category, "5999")
            
            merchants.append({
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "category": category,
                "country_code": country,
                "city": city,
                "mcc_code": mcc_code,
            })
        
        df = self.spark.createDataFrame(merchants)
        logger.info(f"Generated {df.count()} merchants")
        return df
    
    def generate_transactions(self, include_bad_data: bool = True) -> DataFrame:
        """
        Generate transaction data with foreign keys.
        
        Args:
            include_bad_data: Whether to include intentional bad data for testing
        
        Returns:
            DataFrame with transaction records
        """
        if not self._customer_ids or not self._account_ids or not self._merchant_ids:
            raise ValueError("Generate customers, accounts, and merchants first")
        
        logger.info(f"Generating {self.config.num_transactions} transactions")
        
        transactions = []
        
        # Calculate number of bad records
        num_bad_null = int(self.config.num_transactions * self.config.null_pct / 100)
        num_bad_dup = int(self.config.num_transactions * self.config.duplicate_pct / 100)
        num_bad_currency = int(self.config.num_transactions * self.config.invalid_currency_pct / 100)
        num_bad_orphan = int(self.config.num_transactions * self.config.orphan_fk_pct / 100)
        num_bad_negative = int(self.config.num_transactions * self.config.negative_amount_pct / 100)
        num_bad_date = int(self.config.num_transactions * self.config.malformed_date_pct / 100)
        
        duplicate_ids = []  # Track IDs to duplicate
        
        for i in range(self.config.num_transactions):
            transaction_id = self._generate_deterministic_id(f"txn_{i}", "TXN")
            
            # Normal record
            customer_id = self._random_choice(self._customer_ids)
            customer_accounts = self._customer_to_accounts.get(customer_id, [])
            account_id = self._random_choice(customer_accounts) if customer_accounts else self._random_choice(self._account_ids)
            merchant_id = self._random_choice(self._merchant_ids)
            
            txn_datetime = self._random_date(self.config.start_date, self.config.end_date)
            
            amount = round(random.uniform(1, 5000), 2)
            country = self._random_choice(list(COUNTRIES.keys()))
            currency = COUNTRIES[country]["currency"]
            
            transaction_type = self._random_choice([t.value for t in TransactionType])
            status = self._random_choice([s.value for s in TransactionStatus])
            channel = self._random_choice([c.value for c in Channel])
            
            transaction = {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "account_id": account_id,
                "merchant_id": merchant_id,
                "transaction_date": txn_datetime.strftime("%Y-%m-%d"),
                "transaction_timestamp": txn_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                "amount": str(amount),
                "currency": currency,
                "transaction_type": transaction_type,
                "status": status,
                "channel": channel,
                "country_code": country,
                "description": f"{transaction_type} at {merchant_id[:8]}...",
            }
            
            # Inject bad data if enabled
            if include_bad_data:
                # Null values
                if i < num_bad_null:
                    null_field = random.choice(["customer_id", "amount", "currency", "transaction_type"])
                    transaction[null_field] = None
                    transaction["transaction_id"] = f"{BAD_DATA_MARKERS['null_marker']}{transaction_id}"
                
                # Duplicate IDs
                elif i < num_bad_null + num_bad_dup:
                    if duplicate_ids:
                        transaction["transaction_id"] = random.choice(duplicate_ids)
                    else:
                        duplicate_ids.append(transaction_id)
                        transaction["transaction_id"] = f"{BAD_DATA_MARKERS['duplicate_marker']}{transaction_id}"
                
                # Invalid currency
                elif i < num_bad_null + num_bad_dup + num_bad_currency:
                    transaction["currency"] = random.choice(["XXX", "INVALID", "123", ""])
                    transaction["transaction_id"] = f"{BAD_DATA_MARKERS['invalid_currency_marker']}{transaction_id}"
                
                # Orphan foreign keys
                elif i < num_bad_null + num_bad_dup + num_bad_currency + num_bad_orphan:
                    orphan_type = random.choice(["customer", "account", "merchant"])
                    if orphan_type == "customer":
                        transaction["customer_id"] = "ORPHAN_CUST_12345"
                    elif orphan_type == "account":
                        transaction["account_id"] = "ORPHAN_ACCT_12345"
                    else:
                        transaction["merchant_id"] = "ORPHAN_MERCH_12345"
                    transaction["transaction_id"] = f"{BAD_DATA_MARKERS['orphan_fk_marker']}{transaction_id}"
                
                # Negative amounts
                elif i < num_bad_null + num_bad_dup + num_bad_currency + num_bad_orphan + num_bad_negative:
                    transaction["amount"] = str(-abs(amount))
                    transaction["transaction_id"] = f"{BAD_DATA_MARKERS['negative_amount_marker']}{transaction_id}"
                
                # Malformed dates
                elif i < num_bad_null + num_bad_dup + num_bad_currency + num_bad_orphan + num_bad_negative + num_bad_date:
                    malformed = random.choice([
                        "not-a-date",
                        "2024/13/45",
                        "32-01-2024",
                        "",
                        "2024-02-30",  # Invalid Feb date
                    ])
                    transaction["transaction_date"] = malformed
                    transaction["transaction_id"] = f"{BAD_DATA_MARKERS['malformed_date_marker']}{transaction_id}"
            
            transactions.append(transaction)
            
            # Store some IDs for potential duplicates
            if i % 1000 == 0 and include_bad_data:
                duplicate_ids.append(transaction_id)
        
        df = self.spark.createDataFrame(transactions)
        
        # Log statistics
        total = df.count()
        bad_count = df.filter(
            F.col("transaction_id").rlike("^TEST_")
        ).count() if include_bad_data else 0
        
        logger.info(
            f"Generated {total} transactions ({bad_count} intentionally bad for testing)",
            total_transactions=total,
            bad_data_count=bad_count,
        )
        
        return df
    
    def generate_all(self, include_bad_data: bool = True) -> Dict[str, DataFrame]:
        """
        Generate all datasets with referential integrity.
        
        Args:
            include_bad_data: Whether to include bad data for testing
        
        Returns:
            Dictionary of table name -> DataFrame
        """
        logger.info("Starting full data generation")
        
        customers_df = self.generate_customers()
        accounts_df = self.generate_accounts()
        merchants_df = self.generate_merchants()
        transactions_df = self.generate_transactions(include_bad_data)
        
        logger.info("Completed full data generation")
        
        return {
            "customers": customers_df,
            "accounts": accounts_df,
            "merchants": merchants_df,
            "transactions": transactions_df,
        }


def generate_full_dataset(
    spark: SparkSession,
    num_transactions: int = 1000000,
    include_bad_data: bool = True,
    seed: int = 42,
) -> Dict[str, DataFrame]:
    """
    Convenience function to generate full dataset.
    
    Args:
        spark: SparkSession
        num_transactions: Number of transactions to generate
        include_bad_data: Include intentional bad data
        seed: Random seed for reproducibility
    
    Returns:
        Dictionary of table name -> DataFrame
    """
    config = GeneratorConfig(
        num_customers=num_transactions // 20,  # ~50K for 1M transactions
        num_accounts=num_transactions // 10,   # ~100K for 1M transactions
        num_merchants=num_transactions // 100, # ~10K for 1M transactions
        num_transactions=num_transactions,
        seed=seed,
    )
    
    generator = SyntheticDataGenerator(spark, config)
    return generator.generate_all(include_bad_data)
