# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC 
# MAGIC Ingests raw data from source systems into Bronze layer tables.
# MAGIC 
# MAGIC ## Process Overview
# MAGIC 1. Generate or load source data
# MAGIC 2. Add ingestion metadata
# MAGIC 3. Write to Bronze Delta tables
# MAGIC 4. Validate ingestion results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import sys

# Add src to path
sys.path.insert(0, "../")

from src.common.config import load_config
from src.common.logging_utils import get_logger
from src.bronze.bronze_transactions import BronzeTransactionsLoader
from src.bronze.bronze_customers import BronzeCustomersLoader
from src.bronze.bronze_accounts import BronzeAccountsLoader
from src.bronze.bronze_merchants import BronzeMerchantsLoader
from src.generators.synthetic_data_generator import SyntheticDataGenerator, GeneratorConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Load configuration
config = load_config()
logger = get_logger("bronze_ingestion")

print(f"Environment: {config.environment}")
print(f"Catalog: {config.catalog.name}")
print(f"Bronze Schema: {config.schemas.bronze}")

# Batch ID for this run
batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Generate Synthetic Data

# COMMAND ----------

# Configure data generation
generator_config = GeneratorConfig(
    num_customers=50000,
    num_accounts=100000,
    num_merchants=10000,
    num_transactions=1000000,
    null_pct=0.02,           # 2% nulls for testing
    duplicate_pct=0.01,       # 1% duplicates
    invalid_currency_pct=0.005,
    orphan_fk_pct=0.01,
    negative_amount_pct=0.005,
    malformed_date_pct=0.002,
)

generator = SyntheticDataGenerator(spark, generator_config)

# COMMAND ----------

# Generate synthetic data
print("Generating synthetic data...")

customers_df = generator.generate_customers()
print(f"Generated {customers_df.count()} customers")

accounts_df = generator.generate_accounts()
print(f"Generated {accounts_df.count()} accounts")

merchants_df = generator.generate_merchants()
print(f"Generated {merchants_df.count()} merchants")

transactions_df = generator.generate_transactions()
print(f"Generated {transactions_df.count()} transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load to Bronze Tables

# COMMAND ----------

# Initialize loaders
customers_loader = BronzeCustomersLoader(spark, config)
accounts_loader = BronzeAccountsLoader(spark, config)
merchants_loader = BronzeMerchantsLoader(spark, config)
transactions_loader = BronzeTransactionsLoader(spark, config)

# COMMAND ----------

# Ensure tables exist
customers_loader.create_table_if_not_exists()
accounts_loader.create_table_if_not_exists()
merchants_loader.create_table_if_not_exists()
transactions_loader.create_table_if_not_exists()

print("Tables created/verified")

# COMMAND ----------

# Load customers
print("Loading customers...")
customers_count = customers_loader.load_from_dataframe(
    customers_df,
    batch_id=batch_id,
)
print(f"Loaded {customers_count} customers to Bronze")

# COMMAND ----------

# Load accounts
print("Loading accounts...")
accounts_count = accounts_loader.load_from_dataframe(
    accounts_df,
    batch_id=batch_id,
)
print(f"Loaded {accounts_count} accounts to Bronze")

# COMMAND ----------

# Load merchants
print("Loading merchants...")
merchants_count = merchants_loader.load_from_dataframe(
    merchants_df,
    batch_id=batch_id,
)
print(f"Loaded {merchants_count} merchants to Bronze")

# COMMAND ----------

# Load transactions
print("Loading transactions...")
transactions_count = transactions_loader.load_from_dataframe(
    transactions_df,
    batch_id=batch_id,
)
print(f"Loaded {transactions_count} transactions to Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Ingestion

# COMMAND ----------

# Verify record counts
print("\n=== Bronze Layer Summary ===")

tables = [
    (f"{config.catalog.name}.{config.schemas.bronze}.customers_raw", "Customers"),
    (f"{config.catalog.name}.{config.schemas.bronze}.accounts_raw", "Accounts"),
    (f"{config.catalog.name}.{config.schemas.bronze}.merchants_raw", "Merchants"),
    (f"{config.catalog.name}.{config.schemas.bronze}.transactions_raw", "Transactions"),
]

for table_name, label in tables:
    try:
        count = spark.table(table_name).count()
        print(f"{label}: {count:,} records")
    except Exception as e:
        print(f"{label}: Error - {e}")

# COMMAND ----------

# Check batch ingestion
print(f"\n=== Records for Batch: {batch_id} ===")

for table_name, label in tables:
    try:
        batch_count = spark.table(table_name).filter(
            F.col("_batch_id") == batch_id
        ).count()
        print(f"{label}: {batch_count:,} records")
    except Exception as e:
        print(f"{label}: Error - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# Preview transactions
display(
    spark.table(f"{config.catalog.name}.{config.schemas.bronze}.transactions_raw")
    .filter(F.col("_batch_id") == batch_id)
    .limit(10)
)

# COMMAND ----------

print(f"Bronze ingestion completed. Batch ID: {batch_id}")
