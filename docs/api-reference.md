# API Reference

## Common Module

### config.py

#### `Config`
Main configuration dataclass.

```python
@dataclass
class Config:
    environment: str = "dev"
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    schemas: SchemaConfig = field(default_factory=SchemaConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    debug: bool = False
```

#### `load_config(environment: str = None) -> Config`
Load configuration for the specified environment.

**Parameters:**
- `environment`: Optional override. Defaults to `ENVIRONMENT` env var.

**Returns:** `Config` instance.

---

### quality.py

#### `DataQualityValidator`
Performs data quality checks on DataFrames.

```python
class DataQualityValidator:
    def __init__(self, spark: SparkSession)
```

**Methods:**

##### `check_nulls(df, column, threshold) -> QualityCheckResult`
Check null rate for a column.

**Parameters:**
- `df`: DataFrame to check
- `column`: Column name
- `threshold`: Maximum allowed null rate (0.0 to 1.0)

##### `check_duplicates(df, columns, threshold) -> QualityCheckResult`
Check duplicate rate for column combination.

##### `check_range(df, column, min_value, max_value) -> QualityCheckResult`
Check values are within range.

##### `check_enum(df, column, valid_values) -> QualityCheckResult`
Check values are in allowed set.

---

#### `QualityReport`
Aggregates quality check results.

```python
class QualityReport:
    table_name: str
    total_rows: int
    checks: List[QualityCheckResult]
    
    @property
    def overall_passed(self) -> bool
    
    @property
    def passed_count(self) -> int
    
    @property
    def failed_count(self) -> int
```

---

### constants.py

#### Enums

```python
class Layer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"

class TransactionStatus(Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"

class TransactionType(Enum):
    PURCHASE = "PURCHASE"
    REFUND = "REFUND"
    TRANSFER = "TRANSFER"
    WITHDRAWAL = "WITHDRAWAL"
    DEPOSIT = "DEPOSIT"

class Channel(Enum):
    ONLINE = "ONLINE"
    IN_STORE = "IN_STORE"
    MOBILE = "MOBILE"
    ATM = "ATM"
    PHONE = "PHONE"
```

#### Constants

```python
VALID_CURRENCIES = ["USD", "EUR", "GBP", "JPY", ...]  # 20 currencies
COUNTRIES = ["US", "GB", "DE", "FR", ...]  # Country codes
BAD_DATA_MARKERS = ["NULL", "N/A", "UNKNOWN", ...]
```

---

## Bronze Module

### BronzeTransactionsLoader

```python
class BronzeTransactionsLoader:
    def __init__(self, spark: SparkSession, config: Config)
```

**Methods:**

##### `load_from_csv(path, batch_id, options) -> int`
Load transactions from CSV file.

**Parameters:**
- `path`: Path to CSV file(s)
- `batch_id`: Unique batch identifier
- `options`: Optional CSV read options

**Returns:** Number of records loaded.

##### `load_from_json(path, batch_id, options) -> int`
Load transactions from JSON file.

##### `load_from_dataframe(df, batch_id) -> int`
Load transactions from existing DataFrame.

##### `create_table_if_not_exists() -> None`
Create Bronze table with correct schema.

---

## Silver Module

### SilverFinancialsTransformer

```python
class SilverFinancialsTransformer:
    def __init__(self, spark: SparkSession, config: Config)
```

**Methods:**

##### `transform_transactions(incremental=True) -> QualityReport`
Transform Bronze transactions to Silver.

**Parameters:**
- `incremental`: If True, process only new batches

**Returns:** Quality report with validation results.

##### `get_processing_stats() -> dict`
Get statistics about last processing run.

---

### SilverDimensionsTransformer

```python
class SilverDimensionsTransformer:
    def __init__(self, spark: SparkSession, config: Config)
```

**Methods:**

##### `build_dim_customer(scd_type=2) -> int`
Build customer dimension with SCD Type 2.

##### `build_dim_account(scd_type=2) -> int`
Build account dimension with SCD Type 2.

##### `build_dim_merchant() -> int`
Build merchant dimension (SCD Type 1).

##### `build_dim_date(start_year, end_year) -> int`
Build calendar dimension.

---

### QuarantineHandler

```python
class QuarantineHandler:
    def __init__(self, spark: SparkSession, config: Config)
```

**Methods:**

##### `quarantine_records(df, reason) -> int`
Send records to quarantine.

##### `get_quarantine_stats() -> dict`
Get quarantine statistics.

##### `reprocess_quarantine(batch_id) -> DataFrame`
Retrieve quarantined records for reprocessing.

---

## Gold Module

### GoldAnalyticsBuilder

```python
class GoldAnalyticsBuilder:
    def __init__(self, spark: SparkSession, config: Config)
```

**Methods:**

##### `build_all() -> dict`
Build all analytics tables.

**Returns:** Dict of table names to record counts.

##### `build_revenue_summary() -> int`
Build daily revenue summary.

##### `build_customer_analytics() -> int`
Build customer lifetime value and segmentation.

##### `build_merchant_analytics() -> int`
Build merchant performance metrics.

##### `build_channel_performance() -> int`
Build monthly channel performance.

##### `build_geographic_summary() -> int`
Build quarterly geographic summary.

---

### GoldKPIBuilder

```python
class GoldKPIBuilder:
    def __init__(self, spark: SparkSession, config: Config)
```

**Methods:**

##### `build_all() -> dict`
Build all KPI tables.

##### `build_daily_kpis() -> int`
Build daily KPIs with rolling averages.

##### `build_monthly_kpis() -> int`
Build monthly KPIs with MoM/YoY.

##### `build_executive_summary() -> int`
Build executive summary metrics.

---

## Generators Module

### SyntheticDataGenerator

```python
@dataclass
class GeneratorConfig:
    num_customers: int = 50000
    num_accounts: int = 100000
    num_merchants: int = 10000
    num_transactions: int = 1000000
    null_pct: float = 0.02
    duplicate_pct: float = 0.01
    invalid_currency_pct: float = 0.005
    orphan_fk_pct: float = 0.01
    negative_amount_pct: float = 0.005
    malformed_date_pct: float = 0.002

class SyntheticDataGenerator:
    def __init__(self, spark: SparkSession, config: GeneratorConfig)
```

**Methods:**

##### `generate_customers() -> DataFrame`
Generate synthetic customer records.

##### `generate_accounts() -> DataFrame`
Generate synthetic account records.

##### `generate_merchants() -> DataFrame`
Generate synthetic merchant records.

##### `generate_transactions() -> DataFrame`
Generate synthetic transaction records with intentional bad data.
