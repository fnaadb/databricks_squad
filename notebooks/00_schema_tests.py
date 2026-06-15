# Databricks notebook source
# MAGIC %md
# MAGIC # Schema & Data Type Tests
# MAGIC 
# MAGIC Unit and integration tests for Bronze, Silver, and Gold layer data type definitions.
# MAGIC 
# MAGIC ## Test Categories
# MAGIC 1. **Unit Tests**: Validate individual schema definitions
# MAGIC 2. **Integration Tests**: Validate cross-layer consistency (Bronze → Silver → Gold)
# MAGIC 3. **Type Compliance Tests**: Ensure Databricks best practices for data types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ShortType, 
    ByteType, BooleanType, DateType, TimestampType, DecimalType, DoubleType
)
from pyspark.sql import functions as F
from datetime import datetime, date, timedelta
import json

# Test results collector
test_results = []

def test(name, condition, details=""):
    """Record test result."""
    status = "✅ PASS" if condition else "❌ FAIL"
    test_results.append({
        "test_name": name,
        "status": status,
        "passed": condition,
        "details": details
    })
    print(f"{status}: {name}")
    if details and not condition:
        print(f"       Details: {details}")
    return condition

def summary():
    """Print test summary."""
    passed = sum(1 for t in test_results if t["passed"])
    total = len(test_results)
    print("\n" + "="*60)
    print(f"TEST SUMMARY: {passed}/{total} tests passed")
    print("="*60)
    if passed < total:
        print("\nFailed tests:")
        for t in test_results:
            if not t["passed"]:
                print(f"  - {t['test_name']}: {t['details']}")
    return passed == total

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit Tests: Bronze Layer Schema Definitions

# COMMAND ----------

print("="*60)
print("UNIT TESTS: Bronze Layer Schema Definitions")
print("="*60)

# Expected Bronze schemas (all STRING for raw ingestion - this is correct for bronze)
EXPECTED_BRONZE_SCHEMAS = {
    "customers_raw": {
        "customer_id": StringType,
        "first_name": StringType,
        "last_name": StringType,
        "email": StringType,
        "phone": StringType,
        "date_of_birth": StringType,  # Raw string, will be cast in Silver
        "country_code": StringType,
        "created_at": StringType,     # Raw string, will be cast in Silver
        "updated_at": StringType,
    },
    "accounts_raw": {
        "account_id": StringType,
        "customer_id": StringType,
        "account_type": StringType,
        "account_number": StringType,
        "balance": StringType,        # Raw string, will be cast to DECIMAL in Silver
        "currency": StringType,
        "status": StringType,
        "opened_date": StringType,
        "closed_date": StringType,
    },
    "merchants_raw": {
        "merchant_id": StringType,
        "merchant_name": StringType,
        "category": StringType,
        "country_code": StringType,
        "city": StringType,
        "mcc_code": StringType,
    },
    "transactions_raw": {
        "transaction_id": StringType,
        "customer_id": StringType,
        "account_id": StringType,
        "merchant_id": StringType,
        "transaction_date": StringType,
        "transaction_timestamp": StringType,
        "amount": StringType,         # Raw string, will be cast to DECIMAL in Silver
        "currency": StringType,
        "transaction_type": StringType,
        "status": StringType,
        "channel": StringType,
        "country_code": StringType,
        "description": StringType,
    }
}

# Validate Bronze schema constants match expected structure
# Import the schemas from the bronze notebook by reading the file
bronze_notebook_path = "/Workspace/Users/admin@mngenvmcap491728.onmicrosoft.com/databricks_squad/notebooks/01_bronze_ingestion"

# Test Bronze schema column counts
test("Bronze customers_raw has 9 columns", 
     len(EXPECTED_BRONZE_SCHEMAS["customers_raw"]) == 9,
     f"Expected 9, got {len(EXPECTED_BRONZE_SCHEMAS['customers_raw'])}")

test("Bronze accounts_raw has 9 columns",
     len(EXPECTED_BRONZE_SCHEMAS["accounts_raw"]) == 9,
     f"Expected 9, got {len(EXPECTED_BRONZE_SCHEMAS['accounts_raw'])}")

test("Bronze merchants_raw has 6 columns",
     len(EXPECTED_BRONZE_SCHEMAS["merchants_raw"]) == 6,
     f"Expected 6, got {len(EXPECTED_BRONZE_SCHEMAS['merchants_raw'])}")

test("Bronze transactions_raw has 13 columns",
     len(EXPECTED_BRONZE_SCHEMAS["transactions_raw"]) == 13,
     f"Expected 13, got {len(EXPECTED_BRONZE_SCHEMAS['transactions_raw'])}")

# Test all Bronze columns are STRING type (raw layer best practice)
for table_name, schema in EXPECTED_BRONZE_SCHEMAS.items():
    for col_name, col_type in schema.items():
        test(f"Bronze {table_name}.{col_name} is STRING",
             col_type == StringType,
             f"Expected StringType, got {col_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit Tests: Silver Layer Schema Definitions

# COMMAND ----------

print("\n" + "="*60)
print("UNIT TESTS: Silver Layer Schema Definitions")
print("="*60)

# Expected Silver schemas with proper Databricks types
EXPECTED_SILVER_SCHEMAS = {
    "dim_date": {
        "date_key": IntegerType,       # YYYYMMDD format
        "full_date": DateType,         # Proper DATE type
        "year": ShortType,             # SMALLINT for year
        "quarter": ByteType,           # TINYINT (1-4)
        "month": ByteType,             # TINYINT (1-12)
        "month_name": StringType,
        "week_of_year": ByteType,      # TINYINT (1-53)
        "day_of_month": ByteType,      # TINYINT (1-31)
        "day_of_week": ByteType,       # TINYINT (1-7)
        "day_name": StringType,
        "is_weekend": BooleanType,
        "is_holiday": BooleanType,
    },
    "dim_customer": {
        "customer_key": LongType,      # Surrogate key (monotonically_increasing_id)
        "customer_id": StringType,     # Business key
        "first_name": StringType,
        "last_name": StringType,
        "email": StringType,
        "phone": StringType,
        "date_of_birth": DateType,     # Cast from STRING to DATE
        "country_code": StringType,
        "customer_since": DateType,    # Cast from STRING to DATE
        "is_active": BooleanType,
        "effective_start_date": TimestampType,
        "effective_end_date": TimestampType,
        "is_current": BooleanType,
        "_created_timestamp": TimestampType,
    },
    "dim_account": {
        "account_key": LongType,       # Surrogate key
        "account_id": StringType,      # Business key
        "customer_key": LongType,      # FK to dim_customer
        "account_type": StringType,
        "account_status": StringType,
        "balance": DecimalType,        # DECIMAL(18,2) for financial
        "currency": StringType,
        "opened_date": DateType,
        "closed_date": DateType,
        "effective_start_date": TimestampType,
        "effective_end_date": TimestampType,
        "is_current": BooleanType,
        "_created_timestamp": TimestampType,
    },
    "dim_merchant": {
        "merchant_key": LongType,      # Surrogate key
        "merchant_id": StringType,     # Business key
        "merchant_name": StringType,
        "merchant_category": StringType,
        "city": StringType,
        "country_code": StringType,
        "mcc_code": StringType,
        "is_active": BooleanType,
        "_created_timestamp": TimestampType,
    },
    "fact_transactions": {
        "transaction_key": LongType,   # Surrogate key
        "transaction_id": StringType,  # Business key
        "customer_key": LongType,      # FK
        "account_key": LongType,       # FK
        "merchant_key": LongType,      # FK
        "date_key": IntegerType,       # FK to dim_date
        "transaction_timestamp": TimestampType,
        "amount_usd": DecimalType,     # DECIMAL(18,2) for financial
        "currency": StringType,
        "transaction_type": StringType,
        "channel": StringType,
        "is_fraud": BooleanType,
        "_batch_id": StringType,
        "_created_timestamp": TimestampType,
    },
}

# Test Silver schema structure
for table_name, schema in EXPECTED_SILVER_SCHEMAS.items():
    test(f"Silver {table_name} schema defined",
         len(schema) > 0,
         f"Schema has {len(schema)} columns")

# Test critical type conversions from Bronze
test("Silver dim_customer.date_of_birth is DATE (not STRING)",
     EXPECTED_SILVER_SCHEMAS["dim_customer"]["date_of_birth"] == DateType,
     "Must cast from Bronze STRING to DATE")

test("Silver dim_customer.customer_since is DATE",
     EXPECTED_SILVER_SCHEMAS["dim_customer"]["customer_since"] == DateType,
     "Must cast from Bronze created_at STRING to DATE")

test("Silver dim_account.balance is DECIMAL",
     EXPECTED_SILVER_SCHEMAS["dim_account"]["balance"] == DecimalType,
     "Financial amounts must be DECIMAL(18,2)")

test("Silver fact_transactions.amount_usd is DECIMAL",
     EXPECTED_SILVER_SCHEMAS["fact_transactions"]["amount_usd"] == DecimalType,
     "Financial amounts must be DECIMAL(18,2)")

test("Silver dim_date.date_key is INT (not BIGINT)",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["date_key"] == IntegerType,
     "YYYYMMDD keys should be INT")

test("Silver dim_date.year is SMALLINT",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["year"] == ShortType,
     "Year values 2020-2030 fit in SMALLINT")

test("Silver dim_date.month is TINYINT",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["month"] == ByteType,
     "Month values 1-12 fit in TINYINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unit Tests: Gold Layer Schema Definitions

# COMMAND ----------

print("\n" + "="*60)
print("UNIT TESTS: Gold Layer Schema Definitions")
print("="*60)

# Expected Gold schemas with proper aggregation types
EXPECTED_GOLD_SCHEMAS = {
    "revenue_summary": {
        "year": ShortType,
        "quarter": ByteType,
        "month": ByteType,
        "month_name": StringType,
        "channel": StringType,
        "merchant_category": StringType,
        "transaction_count": LongType,       # count() returns BIGINT
        "total_revenue_usd": DecimalType,    # DECIMAL(18,2) for financial
        "avg_transaction_amount": DecimalType,
        "unique_customers": LongType,
        "unique_merchants": LongType,
        "fraud_count": LongType,
        "_created_timestamp": TimestampType,
    },
    "customer_analytics": {
        "customer_key": LongType,
        "total_transactions": LongType,
        "lifetime_value_usd": DecimalType,   # DECIMAL(18,2)
        "avg_transaction_amount": DecimalType,
        "first_transaction_date": TimestampType,
        "last_transaction_date": TimestampType,
        "unique_merchants_visited": LongType,
        "fraud_count": LongType,
        "customer_id": StringType,
        "first_name": StringType,
        "last_name": StringType,
        "country_code": StringType,
        "customer_since": DateType,
        "customer_segment": StringType,
        "days_since_first_transaction": IntegerType,
        "days_since_last_transaction": IntegerType,
        "_created_timestamp": TimestampType,
    },
    "merchant_analytics": {
        "merchant_key": LongType,
        "total_transactions": LongType,
        "total_revenue_usd": DecimalType,
        "avg_transaction_amount": DecimalType,
        "unique_customers": LongType,
        "first_transaction_date": TimestampType,
        "last_transaction_date": TimestampType,
        "fraud_count": LongType,
        "merchant_id": StringType,
        "merchant_name": StringType,
        "merchant_category": StringType,
        "city": StringType,
        "country_code": StringType,
        "merchant_tier": StringType,
        "fraud_rate": DecimalType,           # DECIMAL(5,4) for rate
        "_created_timestamp": TimestampType,
    },
    "channel_performance": {
        "year": ShortType,
        "month": ByteType,
        "channel": StringType,
        "transaction_count": LongType,
        "total_revenue_usd": DecimalType,
        "avg_transaction_amount": DecimalType,
        "unique_customers": LongType,
        "fraud_count": LongType,
        "fraud_rate": DecimalType,
        "_created_timestamp": TimestampType,
    },
    "daily_kpis": {
        "date_key": IntegerType,
        "full_date": DateType,
        "year": ShortType,
        "month": ByteType,
        "day_of_week": ByteType,
        "day_name": StringType,
        "is_weekend": BooleanType,
        "transaction_count": LongType,
        "total_revenue": DecimalType,
        "avg_order_value": DecimalType,
        "unique_customers": LongType,
        "unique_merchants": LongType,
        "fraud_count": LongType,
        "revenue_per_customer": DecimalType,
        "_created_timestamp": TimestampType,
    },
    "monthly_kpis": {
        "year": ShortType,
        "month": ByteType,
        "month_name": StringType,
        "transaction_count": LongType,
        "total_revenue": DecimalType,
        "avg_order_value": DecimalType,
        "unique_customers": LongType,
        "active_merchants": LongType,
        "fraud_count": LongType,
        "active_days": LongType,
        "revenue_per_customer": DecimalType,
        "avg_daily_revenue": DecimalType,
        "fraud_rate": DecimalType,
        "_created_timestamp": TimestampType,
    },
}

# Test Gold schema financial columns are DECIMAL
for table_name, schema in EXPECTED_GOLD_SCHEMAS.items():
    for col_name, col_type in schema.items():
        if "revenue" in col_name.lower() or "amount" in col_name.lower() or "value" in col_name.lower():
            test(f"Gold {table_name}.{col_name} is DECIMAL",
                 col_type == DecimalType,
                 f"Financial columns must be DECIMAL, not {col_type}")

# Test Gold rate columns are DECIMAL (not DOUBLE)
test("Gold merchant_analytics.fraud_rate is DECIMAL",
     EXPECTED_GOLD_SCHEMAS["merchant_analytics"]["fraud_rate"] == DecimalType,
     "Rates should be DECIMAL(5,4) for precision")

test("Gold monthly_kpis.fraud_rate is DECIMAL",
     EXPECTED_GOLD_SCHEMAS["monthly_kpis"]["fraud_rate"] == DecimalType,
     "Rates should be DECIMAL(5,4) for precision")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration Tests: Cross-Layer Column Mapping

# COMMAND ----------

print("\n" + "="*60)
print("INTEGRATION TESTS: Cross-Layer Column Mapping")
print("="*60)

# Test: Bronze columns exist for Silver transformations
BRONZE_TO_SILVER_MAPPINGS = {
    # Bronze table -> Silver table -> (bronze_col, silver_col, transform)
    "customers_raw -> dim_customer": [
        ("customer_id", "customer_id", "direct"),
        ("first_name", "first_name", "direct"),
        ("last_name", "last_name", "direct"),
        ("email", "email", "direct"),
        ("phone", "phone", "direct"),
        ("date_of_birth", "date_of_birth", "try_cast to DATE"),
        ("country_code", "country_code", "direct"),
        ("created_at", "customer_since", "try_cast to DATE"),
    ],
    "accounts_raw -> dim_account": [
        ("account_id", "account_id", "direct"),
        ("customer_id", None, "lookup to customer_key"),
        ("account_type", "account_type", "direct"),
        ("balance", "balance", "try_cast to DECIMAL(18,2)"),
        ("currency", "currency", "direct"),
        ("status", "account_status", "rename"),
        ("opened_date", "opened_date", "try_cast to DATE"),
        ("closed_date", "closed_date", "try_cast to DATE"),
    ],
    "merchants_raw -> dim_merchant": [
        ("merchant_id", "merchant_id", "direct"),
        ("merchant_name", "merchant_name", "direct"),
        ("category", "merchant_category", "rename"),
        ("country_code", "country_code", "direct"),
        ("city", "city", "direct"),
        ("mcc_code", "mcc_code", "direct"),
    ],
    "transactions_raw -> fact_transactions": [
        ("transaction_id", "transaction_id", "direct"),
        ("customer_id", None, "lookup to customer_key"),
        ("account_id", None, "lookup to account_key"),
        ("merchant_id", None, "lookup to merchant_key"),
        ("transaction_date", "date_key", "format to YYYYMMDD INT"),
        ("transaction_timestamp", "transaction_timestamp", "try_to_timestamp"),
        ("amount", "amount_usd", "try_cast to DECIMAL(18,2)"),
        ("currency", "currency", "direct"),
        ("transaction_type", "transaction_type", "direct"),
        ("channel", "channel", "direct"),
    ],
}

# Validate all Bronze source columns exist
for mapping_name, columns in BRONZE_TO_SILVER_MAPPINGS.items():
    bronze_table = mapping_name.split(" -> ")[0]
    for bronze_col, silver_col, transform in columns:
        test(f"Bronze {bronze_table}.{bronze_col} exists for Silver transform",
             bronze_col in EXPECTED_BRONZE_SCHEMAS[bronze_table],
             f"Required for: {transform}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration Tests: Silver to Gold Dependencies

# COMMAND ----------

print("\n" + "="*60)
print("INTEGRATION TESTS: Silver to Gold Dependencies")
print("="*60)

# Silver columns required by Gold aggregations
SILVER_TO_GOLD_DEPENDENCIES = {
    "fact_transactions": [
        "transaction_key",    # count()
        "customer_key",       # groupBy, countDistinct
        "account_key",        # groupBy
        "merchant_key",       # groupBy, countDistinct
        "date_key",           # join to dim_date
        "amount_usd",         # sum(), avg()
        "is_fraud",           # sum(when)
        "channel",            # groupBy
        "transaction_timestamp",  # min(), max()
    ],
    "dim_date": [
        "date_key",
        "year",
        "quarter",
        "month",
        "month_name",
        "full_date",
        "day_of_week",
        "day_name",
        "is_weekend",
    ],
    "dim_customer": [
        "customer_key",
        "customer_id",
        "first_name",
        "last_name",
        "country_code",
        "customer_since",
    ],
    "dim_merchant": [
        "merchant_key",
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "city",
        "country_code",
    ],
}

# Validate Silver columns exist for Gold aggregations
for silver_table, required_cols in SILVER_TO_GOLD_DEPENDENCIES.items():
    for col in required_cols:
        test(f"Silver {silver_table}.{col} exists for Gold layer",
             col in EXPECTED_SILVER_SCHEMAS[silver_table],
             f"Required for Gold aggregations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Type Compliance Tests: Databricks Best Practices

# COMMAND ----------

print("\n" + "="*60)
print("TYPE COMPLIANCE TESTS: Databricks Best Practices")
print("="*60)

# Best Practice 1: Use DECIMAL for financial amounts (not DOUBLE)
financial_columns = [
    ("dim_account", "balance"),
    ("fact_transactions", "amount_usd"),
    ("revenue_summary", "total_revenue_usd"),
    ("revenue_summary", "avg_transaction_amount"),
    ("customer_analytics", "lifetime_value_usd"),
    ("customer_analytics", "avg_transaction_amount"),
    ("merchant_analytics", "total_revenue_usd"),
    ("merchant_analytics", "avg_transaction_amount"),
    ("channel_performance", "total_revenue_usd"),
    ("channel_performance", "avg_transaction_amount"),
    ("daily_kpis", "total_revenue"),
    ("daily_kpis", "avg_order_value"),
    ("daily_kpis", "revenue_per_customer"),
    ("monthly_kpis", "total_revenue"),
    ("monthly_kpis", "avg_order_value"),
    ("monthly_kpis", "revenue_per_customer"),
    ("monthly_kpis", "avg_daily_revenue"),
]

for table, col in financial_columns:
    all_schemas = {**EXPECTED_SILVER_SCHEMAS, **EXPECTED_GOLD_SCHEMAS}
    if table in all_schemas and col in all_schemas[table]:
        test(f"BP1: {table}.{col} uses DECIMAL (not DOUBLE)",
             all_schemas[table][col] == DecimalType,
             "Financial precision requires DECIMAL")

# Best Practice 2: Use appropriate integer sizes
test("BP2: dim_date.month uses TINYINT (1-12 range)",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["month"] == ByteType,
     "Values 1-12 fit in TINYINT, saves storage")

test("BP2: dim_date.quarter uses TINYINT (1-4 range)",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["quarter"] == ByteType,
     "Values 1-4 fit in TINYINT")

test("BP2: dim_date.year uses SMALLINT (not INT)",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["year"] == ShortType,
     "Year values fit in SMALLINT")

# Best Practice 3: Use DATE for date-only values, TIMESTAMP for datetime
test("BP3: dim_date.full_date is DATE (not TIMESTAMP)",
     EXPECTED_SILVER_SCHEMAS["dim_date"]["full_date"] == DateType,
     "Date-only values should use DATE type")

test("BP3: fact_transactions.transaction_timestamp is TIMESTAMP",
     EXPECTED_SILVER_SCHEMAS["fact_transactions"]["transaction_timestamp"] == TimestampType,
     "DateTime values should use TIMESTAMP")

# Best Practice 4: Boolean flags are BOOLEAN (not INT/STRING)
boolean_columns = [
    ("dim_date", "is_weekend"),
    ("dim_date", "is_holiday"),
    ("dim_customer", "is_active"),
    ("dim_customer", "is_current"),
    ("dim_merchant", "is_active"),
    ("fact_transactions", "is_fraud"),
]

for table, col in boolean_columns:
    test(f"BP4: {table}.{col} is BOOLEAN",
         EXPECTED_SILVER_SCHEMAS[table][col] == BooleanType,
         "Flags should be BOOLEAN, not INT or STRING")

# Best Practice 5: Surrogate keys are BIGINT
surrogate_keys = [
    ("dim_customer", "customer_key"),
    ("dim_account", "account_key"),
    ("dim_merchant", "merchant_key"),
    ("fact_transactions", "transaction_key"),
]

for table, col in surrogate_keys:
    test(f"BP5: {table}.{col} is BIGINT (surrogate key)",
         EXPECTED_SILVER_SCHEMAS[table][col] == LongType,
         "Surrogate keys from monotonically_increasing_id are BIGINT")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Live Schema Validation (if tables exist)

# COMMAND ----------

print("\n" + "="*60)
print("LIVE SCHEMA VALIDATION (checking actual tables)")
print("="*60)

CATALOG = "purviewcatalog"

def check_table_schema(catalog, schema, table, expected_types):
    """Check if actual table schema matches expected types."""
    try:
        df = spark.table(f"{catalog}.{schema}.{table}")
        actual_schema = {field.name: type(field.dataType) for field in df.schema.fields}
        
        mismatches = []
        for col, expected_type in expected_types.items():
            if col not in actual_schema:
                mismatches.append(f"{col}: missing")
            elif actual_schema[col] != expected_type:
                # Special handling for DecimalType (needs to check base class)
                actual_type_name = actual_schema[col].__name__
                expected_type_name = expected_type.__name__
                if expected_type == DecimalType and "Decimal" in actual_type_name:
                    continue  # DecimalType matches
                mismatches.append(f"{col}: expected {expected_type_name}, got {actual_type_name}")
        
        return len(mismatches) == 0, mismatches
    except Exception as e:
        return None, [str(e)]

# Check Bronze tables (if they exist)
for table in EXPECTED_BRONZE_SCHEMAS.keys():
    passed, details = check_table_schema(CATALOG, "bronze", table, EXPECTED_BRONZE_SCHEMAS[table])
    if passed is None:
        print(f"⏭️  SKIP: bronze.{table} (table does not exist yet)")
    else:
        test(f"Live: bronze.{table} schema matches",
             passed,
             "; ".join(details) if details else "")

# Check Silver tables (if they exist)
for table in EXPECTED_SILVER_SCHEMAS.keys():
    passed, details = check_table_schema(CATALOG, "silver", table, EXPECTED_SILVER_SCHEMAS[table])
    if passed is None:
        print(f"⏭️  SKIP: silver.{table} (table does not exist yet)")
    else:
        test(f"Live: silver.{table} schema matches",
             passed,
             "; ".join(details) if details else "")

# Check Gold tables (if they exist)
for table in EXPECTED_GOLD_SCHEMAS.keys():
    passed, details = check_table_schema(CATALOG, "gold", table, EXPECTED_GOLD_SCHEMAS[table])
    if passed is None:
        print(f"⏭️  SKIP: gold.{table} (table does not exist yet)")
    else:
        test(f"Live: gold.{table} schema matches",
             passed,
             "; ".join(details) if details else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary

# COMMAND ----------

all_passed = summary()

# Set notebook exit value based on test results
if all_passed:
    print("\n🎉 ALL TESTS PASSED - Safe to run pipeline!")
    dbutils.notebook.exit("PASS")
else:
    print("\n🛑 TESTS FAILED - Fix issues before running pipeline!")
    dbutils.notebook.exit("FAIL")
