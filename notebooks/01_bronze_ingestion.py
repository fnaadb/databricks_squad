# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC 
# MAGIC Ingests raw data from source systems into Bronze layer tables.
# MAGIC 
# MAGIC ## Process Overview
# MAGIC 1. Generate synthetic data with referential integrity
# MAGIC 2. Add ingestion metadata
# MAGIC 3. Write to Bronze Delta tables
# MAGIC 4. Validate ingestion results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import string
import uuid
import hashlib

# Configuration
CATALOG = "squad"
BRONZE_SCHEMA = "bronze"

# Ensure catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

# Batch ID for this run
batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
print(f"Catalog: {CATALOG}")
print(f"Schema: {BRONZE_SCHEMA}")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants & Reference Data

# COMMAND ----------

# Countries with currencies
COUNTRIES = {
    "US": {"name": "United States", "currency": "USD"},
    "GB": {"name": "United Kingdom", "currency": "GBP"},
    "DE": {"name": "Germany", "currency": "EUR"},
    "FR": {"name": "France", "currency": "EUR"},
    "JP": {"name": "Japan", "currency": "JPY"},
    "CA": {"name": "Canada", "currency": "CAD"},
    "AU": {"name": "Australia", "currency": "AUD"},
    "MX": {"name": "Mexico", "currency": "MXN"},
    "BR": {"name": "Brazil", "currency": "BRL"},
    "IN": {"name": "India", "currency": "INR"},
}

VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "MXN", "BRL", "KRW", "SGD", "HKD", "NOK", "SEK", "DKK", "NZD", "ZAR", "AED"]

ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT", "INVESTMENT", "MONEY_MARKET"]
TRANSACTION_TYPES = ["PURCHASE", "REFUND", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "PAYMENT", "FEE", "INTEREST"]
TRANSACTION_STATUSES = ["COMPLETED", "PENDING", "FAILED", "CANCELLED", "REVERSED"]
CHANNELS = ["ONLINE", "MOBILE", "IN_STORE", "ATM", "PHONE", "BRANCH"]
MERCHANT_CATEGORIES = ["RETAIL", "GROCERY", "RESTAURANT", "GAS", "TRAVEL", "ENTERTAINMENT", "UTILITIES", "HEALTHCARE", "EDUCATION", "OTHER"]

BAD_DATA_MARKERS = {
    "null_marker": "TEST_NULL_",
    "duplicate_marker": "TEST_DUP_",
    "invalid_currency_marker": "TEST_INVCUR_",
    "orphan_fk_marker": "TEST_ORPHAN_",
    "negative_amount_marker": "TEST_NEG_",
    "malformed_date_marker": "TEST_BADDATE_",
}

# Names for data generation
FIRST_NAMES = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
MERCHANT_NAMES = ["Walmart", "Amazon", "Costco", "Target", "Kroger", "Walgreens", "CVS", "Home Depot", "Starbucks", "McDonald's", "Subway", "Taco Bell", "Uber", "Netflix", "Spotify", "Apple", "Shell", "Exxon", "United Airlines", "Marriott"]
CITIES = ["NYC", "LA", "CHI", "HOU", "PHX", "PHI", "SA", "SD", "DAL", "SJ"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Configuration

# COMMAND ----------

# Generation parameters
NUM_CUSTOMERS = 50000
NUM_ACCOUNTS = 100000
NUM_MERCHANTS = 10000
NUM_TRANSACTIONS = 1000000

# Date range
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 12, 31)

# Bad data percentages
NULL_PCT = 0.02
DUPLICATE_PCT = 0.01
INVALID_CURRENCY_PCT = 0.005
ORPHAN_FK_PCT = 0.01
NEGATIVE_AMOUNT_PCT = 0.005
MALFORMED_DATE_PCT = 0.002

# Set seed for reproducibility
random.seed(42)

print(f"Will generate:")
print(f"  - {NUM_CUSTOMERS:,} customers")
print(f"  - {NUM_ACCOUNTS:,} accounts")
print(f"  - {NUM_MERCHANTS:,} merchants")
print(f"  - {NUM_TRANSACTIONS:,} transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def generate_id(prefix=""):
    """Generate unique ID with prefix."""
    return f"{prefix}{uuid.uuid4().hex[:12].upper()}"

def deterministic_id(seed_value, prefix=""):
    """Generate deterministic ID from seed."""
    hash_val = hashlib.md5(seed_value.encode()).hexdigest()[:12].upper()
    return f"{prefix}{hash_val}"

def random_date(start, end):
    """Generate random datetime between start and end."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers

# COMMAND ----------

print("Generating customers...")

# Store IDs for referential integrity
customer_ids = []
customer_to_accounts = {}

customers_data = []
for i in range(NUM_CUSTOMERS):
    customer_id = deterministic_id(f"cust_{i}", "CUST")
    customer_ids.append(customer_id)
    
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    country = random.choice(list(COUNTRIES.keys()))
    
    email_domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "company.com"])
    email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{email_domain}"
    phone = f"+1{random.randint(200, 999)}{random.randint(1000000, 9999999)}"
    
    dob = random_date(datetime(1950, 1, 1), datetime(2000, 12, 31))
    created_at = random_date(START_DATE - timedelta(days=365), START_DATE)
    
    customers_data.append({
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

customers_df = spark.createDataFrame(customers_data)
print(f"Generated {customers_df.count():,} customers")
customers_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Accounts (with FK to Customers)

# COMMAND ----------

print("Generating accounts...")

account_ids = []
accounts_data = []
accounts_per_customer = NUM_ACCOUNTS // len(customer_ids) + 1

for customer_id in customer_ids:
    num_accounts = random.randint(1, min(3, accounts_per_customer))
    customer_accounts = []
    
    for j in range(num_accounts):
        if len(accounts_data) >= NUM_ACCOUNTS:
            break
        
        account_id = deterministic_id(f"acct_{customer_id}_{j}", "ACCT")
        customer_accounts.append(account_id)
        account_ids.append(account_id)
        
        account_type = random.choice(ACCOUNT_TYPES)
        country = random.choice(list(COUNTRIES.keys()))
        currency = COUNTRIES[country]["currency"]
        
        opened_date = random_date(START_DATE - timedelta(days=730), START_DATE)
        
        accounts_data.append({
            "account_id": account_id,
            "customer_id": customer_id,  # FK to customers
            "account_type": account_type,
            "account_number": ''.join(random.choices(string.digits, k=16)),
            "balance": str(round(random.uniform(100, 100000), 2)),
            "currency": currency,
            "status": "ACTIVE",
            "opened_date": opened_date.strftime("%Y-%m-%d"),
            "closed_date": None,
        })
    
    customer_to_accounts[customer_id] = customer_accounts

accounts_df = spark.createDataFrame(accounts_data)
print(f"Generated {accounts_df.count():,} accounts")
accounts_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Merchants

# COMMAND ----------

print("Generating merchants...")

merchant_ids = []
merchants_data = []

MCC_MAPPING = {
    "RETAIL": "5411", "GROCERY": "5411", "RESTAURANT": "5812",
    "GAS": "5541", "TRAVEL": "4722", "ENTERTAINMENT": "7922",
    "UTILITIES": "4900", "HEALTHCARE": "8011", "EDUCATION": "8220", "OTHER": "5999",
}

for i in range(NUM_MERCHANTS):
    merchant_id = deterministic_id(f"merch_{i}", "MERCH")
    merchant_ids.append(merchant_id)
    
    base_name = random.choice(MERCHANT_NAMES)
    city = random.choice(CITIES)
    merchant_name = f"{base_name} - {city} #{random.randint(1, 999)}"
    
    category = random.choice(MERCHANT_CATEGORIES)
    country = random.choice(list(COUNTRIES.keys()))
    mcc_code = MCC_MAPPING.get(category, "5999")
    
    merchants_data.append({
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "category": category,
        "country_code": country,
        "city": city,
        "mcc_code": mcc_code,
    })

merchants_df = spark.createDataFrame(merchants_data)
print(f"Generated {merchants_df.count():,} merchants")
merchants_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Transactions (with FK to all dimensions)

# COMMAND ----------

print(f"Generating {NUM_TRANSACTIONS:,} transactions...")

# Calculate bad data counts
num_bad_null = int(NUM_TRANSACTIONS * NULL_PCT)
num_bad_dup = int(NUM_TRANSACTIONS * DUPLICATE_PCT)
num_bad_currency = int(NUM_TRANSACTIONS * INVALID_CURRENCY_PCT)
num_bad_orphan = int(NUM_TRANSACTIONS * ORPHAN_FK_PCT)
num_bad_negative = int(NUM_TRANSACTIONS * NEGATIVE_AMOUNT_PCT)
num_bad_date = int(NUM_TRANSACTIONS * MALFORMED_DATE_PCT)

print(f"Including intentional bad data:")
print(f"  - {num_bad_null:,} null values")
print(f"  - {num_bad_dup:,} duplicates")
print(f"  - {num_bad_currency:,} invalid currencies")
print(f"  - {num_bad_orphan:,} orphan FKs")
print(f"  - {num_bad_negative:,} negative amounts")
print(f"  - {num_bad_date:,} malformed dates")

# COMMAND ----------

transactions_data = []
duplicate_ids = []

for i in range(NUM_TRANSACTIONS):
    transaction_id = deterministic_id(f"txn_{i}", "TXN")
    
    # Valid FK references (referential integrity)
    customer_id = random.choice(customer_ids)
    customer_accounts = customer_to_accounts.get(customer_id, [])
    account_id = random.choice(customer_accounts) if customer_accounts else random.choice(account_ids)
    merchant_id = random.choice(merchant_ids)
    
    txn_datetime = random_date(START_DATE, END_DATE)
    amount = round(random.uniform(1, 5000), 2)
    country = random.choice(list(COUNTRIES.keys()))
    currency = COUNTRIES[country]["currency"]
    
    transaction_type = random.choice(TRANSACTION_TYPES)
    status = random.choice(TRANSACTION_STATUSES)
    channel = random.choice(CHANNELS)
    
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
    
    # Inject bad data for testing data quality
    if i < num_bad_null:
        null_field = random.choice(["customer_id", "amount", "currency", "transaction_type"])
        transaction[null_field] = None
        transaction["transaction_id"] = f"{BAD_DATA_MARKERS['null_marker']}{transaction_id}"
    elif i < num_bad_null + num_bad_dup:
        if duplicate_ids:
            transaction["transaction_id"] = random.choice(duplicate_ids)
        else:
            duplicate_ids.append(transaction_id)
            transaction["transaction_id"] = f"{BAD_DATA_MARKERS['duplicate_marker']}{transaction_id}"
    elif i < num_bad_null + num_bad_dup + num_bad_currency:
        transaction["currency"] = random.choice(["XXX", "INVALID", "123", ""])
        transaction["transaction_id"] = f"{BAD_DATA_MARKERS['invalid_currency_marker']}{transaction_id}"
    elif i < num_bad_null + num_bad_dup + num_bad_currency + num_bad_orphan:
        orphan_type = random.choice(["customer", "account", "merchant"])
        if orphan_type == "customer":
            transaction["customer_id"] = "ORPHAN_CUST_12345"
        elif orphan_type == "account":
            transaction["account_id"] = "ORPHAN_ACCT_12345"
        else:
            transaction["merchant_id"] = "ORPHAN_MERCH_12345"
        transaction["transaction_id"] = f"{BAD_DATA_MARKERS['orphan_fk_marker']}{transaction_id}"
    elif i < num_bad_null + num_bad_dup + num_bad_currency + num_bad_orphan + num_bad_negative:
        transaction["amount"] = str(-abs(amount))
        transaction["transaction_id"] = f"{BAD_DATA_MARKERS['negative_amount_marker']}{transaction_id}"
    elif i < num_bad_null + num_bad_dup + num_bad_currency + num_bad_orphan + num_bad_negative + num_bad_date:
        transaction["transaction_date"] = random.choice(["not-a-date", "2024/13/45", "32-01-2024", "", "2024-02-30"])
        transaction["transaction_id"] = f"{BAD_DATA_MARKERS['malformed_date_marker']}{transaction_id}"
    
    # Store for potential duplicates
    if i % 1000 == 0:
        duplicate_ids.append(transaction_id)
    
    transactions_data.append(transaction)
    
    # Progress indicator
    if (i + 1) % 200000 == 0:
        print(f"  Generated {i + 1:,} transactions...")

transactions_df = spark.createDataFrame(transactions_data)
print(f"Generated {transactions_df.count():,} transactions")

# COMMAND ----------

# Show sample of good and bad data
print("Sample transactions (including intentional bad data):")
transactions_df.filter(F.col("transaction_id").startswith("TEST_")).show(5, truncate=False)
print("\nSample valid transactions:")
transactions_df.filter(~F.col("transaction_id").startswith("TEST_")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Ingestion Metadata

# COMMAND ----------

def add_ingestion_metadata(df, batch_id):
    """Add standard ingestion metadata columns."""
    return df.withColumn("_ingested_at", F.current_timestamp()) \
             .withColumn("_batch_id", F.lit(batch_id)) \
             .withColumn("_source_file", F.lit("synthetic_generator"))

customers_df = add_ingestion_metadata(customers_df, batch_id)
accounts_df = add_ingestion_metadata(accounts_df, batch_id)
merchants_df = add_ingestion_metadata(merchants_df, batch_id)
transactions_df = add_ingestion_metadata(transactions_df, batch_id)

print("Added ingestion metadata to all DataFrames")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Tables

# COMMAND ----------

# Write customers
customers_table = f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
print(f"Writing customers to {customers_table}...")
customers_df.write.format("delta").mode("overwrite").saveAsTable(customers_table)
print(f"  ✓ Written {spark.table(customers_table).count():,} customers")

# COMMAND ----------

# Write accounts
accounts_table = f"{CATALOG}.{BRONZE_SCHEMA}.accounts_raw"
print(f"Writing accounts to {accounts_table}...")
accounts_df.write.format("delta").mode("overwrite").saveAsTable(accounts_table)
print(f"  ✓ Written {spark.table(accounts_table).count():,} accounts")

# COMMAND ----------

# Write merchants
merchants_table = f"{CATALOG}.{BRONZE_SCHEMA}.merchants_raw"
print(f"Writing merchants to {merchants_table}...")
merchants_df.write.format("delta").mode("overwrite").saveAsTable(merchants_table)
print(f"  ✓ Written {spark.table(merchants_table).count():,} merchants")

# COMMAND ----------

# Write transactions
transactions_table = f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw"
print(f"Writing transactions to {transactions_table}...")
transactions_df.write.format("delta").mode("overwrite").saveAsTable(transactions_table)
print(f"  ✓ Written {spark.table(transactions_table).count():,} transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Ingestion & Referential Integrity

# COMMAND ----------

print("\n" + "="*60)
print("BRONZE LAYER INGESTION SUMMARY")
print("="*60)

tables = [
    (f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw", "Customers"),
    (f"{CATALOG}.{BRONZE_SCHEMA}.accounts_raw", "Accounts"),
    (f"{CATALOG}.{BRONZE_SCHEMA}.merchants_raw", "Merchants"),
    (f"{CATALOG}.{BRONZE_SCHEMA}.transactions_raw", "Transactions"),
]

for table_name, label in tables:
    count = spark.table(table_name).count()
    print(f"  {label:15}: {count:>12,} records")

# COMMAND ----------

# Validate referential integrity
print("\n" + "="*60)
print("REFERENTIAL INTEGRITY CHECK")
print("="*60)

# Check accounts -> customers FK
orphan_accounts = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {CATALOG}.{BRONZE_SCHEMA}.accounts_raw a
    LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.customers_raw c 
        ON a.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").collect()[0]["orphan_count"]
print(f"  Accounts without valid customer: {orphan_accounts}")

# Check transactions -> customers FK
orphan_txn_cust = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {CATALOG}.{BRONZE_SCHEMA}.transactions_raw t
    LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.customers_raw c 
        ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").collect()[0]["orphan_count"]
print(f"  Transactions without valid customer: {orphan_txn_cust} (expected ~{num_bad_orphan//3})")

# Check transactions -> accounts FK  
orphan_txn_acct = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {CATALOG}.{BRONZE_SCHEMA}.transactions_raw t
    LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.accounts_raw a 
        ON t.account_id = a.account_id
    WHERE a.account_id IS NULL
""").collect()[0]["orphan_count"]
print(f"  Transactions without valid account: {orphan_txn_acct} (expected ~{num_bad_orphan//3})")

# Check transactions -> merchants FK
orphan_txn_merch = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {CATALOG}.{BRONZE_SCHEMA}.transactions_raw t
    LEFT JOIN {CATALOG}.{BRONZE_SCHEMA}.merchants_raw m 
        ON t.merchant_id = m.merchant_id
    WHERE m.merchant_id IS NULL
""").collect()[0]["orphan_count"]
print(f"  Transactions without valid merchant: {orphan_txn_merch} (expected ~{num_bad_orphan//3})")

# COMMAND ----------

# Count bad data by type
print("\n" + "="*60)
print("INTENTIONAL BAD DATA SUMMARY")
print("="*60)

bad_data_counts = spark.sql(f"""
    SELECT 
        SUM(CASE WHEN transaction_id LIKE 'TEST_NULL_%' THEN 1 ELSE 0 END) as null_records,
        SUM(CASE WHEN transaction_id LIKE 'TEST_DUP_%' THEN 1 ELSE 0 END) as duplicate_markers,
        SUM(CASE WHEN transaction_id LIKE 'TEST_INVCUR_%' THEN 1 ELSE 0 END) as invalid_currency,
        SUM(CASE WHEN transaction_id LIKE 'TEST_ORPHAN_%' THEN 1 ELSE 0 END) as orphan_fk,
        SUM(CASE WHEN transaction_id LIKE 'TEST_NEG_%' THEN 1 ELSE 0 END) as negative_amount,
        SUM(CASE WHEN transaction_id LIKE 'TEST_BADDATE_%' THEN 1 ELSE 0 END) as malformed_date
    FROM {CATALOG}.{BRONZE_SCHEMA}.transactions_raw
""").collect()[0]

print(f"  Null value records:      {bad_data_counts['null_records']:,}")
print(f"  Duplicate markers:       {bad_data_counts['duplicate_markers']:,}")
print(f"  Invalid currency:        {bad_data_counts['invalid_currency']:,}")
print(f"  Orphan foreign keys:     {bad_data_counts['orphan_fk']:,}")
print(f"  Negative amounts:        {bad_data_counts['negative_amount']:,}")
print(f"  Malformed dates:         {bad_data_counts['malformed_date']:,}")

total_bad = sum([bad_data_counts[k] for k in bad_data_counts.asDict().keys()])
print(f"\n  Total bad records:       {total_bad:,}")
print(f"  Good records:            {NUM_TRANSACTIONS - total_bad:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze Ingestion Complete
# MAGIC 
# MAGIC **Next step:** Run `02_silver_transforms` to transform Bronze data into Silver layer.
