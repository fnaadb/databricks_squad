# Databricks Squad - Medallion Architecture

[![CI](https://github.com/fnaadb/databricks_squad/actions/workflows/ci.yml/badge.svg)](https://github.com/fnaadb/databricks_squad/actions/workflows/ci.yml)
[![Deploy](https://github.com/fnaadb/databricks_squad/actions/workflows/deploy.yml/badge.svg)](https://github.com/fnaadb/databricks_squad/actions/workflows/deploy.yml)

Production-quality medallion architecture (Bronze/Silver/Gold) implementation for Databricks using PySpark and Delta Lake.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
│  (CSV, JSON, APIs, Streaming)                                           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw)                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ transactions │  │  customers   │  │   accounts   │  │  merchants   │ │
│  │    _raw      │  │    _raw      │  │    _raw      │  │    _raw      │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
│  • Raw data as-is  • Append-only  • Ingestion metadata  • Bad data kept │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Curated)                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │dim_customer  │  │ dim_account  │  │ dim_merchant │  │   dim_date   │ │
│  │  (SCD-2)     │  │   (SCD-2)    │  │   (SCD-1)    │  │   (Static)   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
│  ┌──────────────────────────────────┐  ┌──────────────────────────────┐ │
│  │       fact_transactions          │  │         quarantine           │ │
│  │  (Cleaned, typed, normalized)    │  │    (Rejected records)        │ │
│  └──────────────────────────────────┘  └──────────────────────────────┘ │
│  • Quality validated  • Type-safe  • USD normalized  • Surrogate keys  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Analytics)                                                  │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────────┐│
│  │  revenue_summary  │  │customer_analytics │  │ merchant_analytics   ││
│  └───────────────────┘  └───────────────────┘  └───────────────────────┘│
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────────┐│
│  │channel_performance│  │geographic_summary │  │ executive_summary    ││
│  └───────────────────┘  └───────────────────┘  └───────────────────────┘│
│  ┌───────────────────┐  ┌───────────────────┐                           │
│  │    daily_kpis     │  │   monthly_kpis    │                           │
│  └───────────────────┘  └───────────────────┘                           │
│  • Business aggregations  • KPIs & trends  • Ready for BI               │
└─────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.9+
- Databricks CLI configured

### Installation

```bash
# Clone the repository
git clone https://github.com/fnaadb/databricks_squad.git
cd databricks_squad

# Install dependencies
pip install -r requirements.txt

# Validate bundle configuration
databricks bundle validate
```

### Generate Synthetic Data

```bash
# Generate 1M+ transaction records
python -m src.generators.synthetic_data_generator --records 1000000
```

### Deploy to Databricks

```bash
# Deploy to development environment
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### Run the Pipeline

```python
# In Databricks notebook or job
from src.bronze.bronze_transactions import BronzeIngestion
from src.silver.silver_financials import SilverTransforms
from src.gold.gold_analytics import GoldAnalytics

# Run Bronze ingestion
bronze = BronzeIngestion(spark, config)
bronze.ingest_transactions()

# Run Silver transformations
silver = SilverTransforms(spark, config)
silver.process_transactions()

# Run Gold aggregations
gold = GoldAnalytics(spark, config)
gold.build_kpis()
```

## Project Structure

```
databricks_squad/
├── README.md
├── CONTRIBUTING.md
├── architecture.md
├── databricks.yml              # Databricks Asset Bundle config
├── pyproject.toml
├── requirements.txt
├── .gitignore
├── .github/
│   ├── pull_request_template.md
│   ├── ISSUE_TEMPLATE/
│   └── workflows/
├── configs/
│   ├── dev.yml
│   ├── test.yml
│   └── prod.yml
├── schemas/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── data/
│   ├── synthetic/
│   └── sample/
├── src/
│   ├── common/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── generators/
├── notebooks/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── data_quality/
│   └── smoke/
└── docs/
```

## Namespace Convention

### Unity Catalog (Preferred)

```
squad.bronze.<table>
squad.silver.<table>
squad.gold.<table>
```

### Hive Metastore Fallback

```
bronze.<table>
silver.<table>
gold.<table>
```

## Data Flow

| Layer | Purpose | Tables |
|-------|---------|--------|
| **Bronze** | Raw ingestion | `transactions_raw`, `customers_raw`, `accounts_raw`, `merchants_raw` |
| **Silver** | Cleansed & conformed | `fact_transactions`, `dim_customer`, `dim_account`, `dim_merchant`, `quarantine_*` |
| **Gold** | Business analytics | `daily_transaction_kpis`, `monthly_financial_summary`, `merchant_performance` |

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run unit tests only
pytest tests/unit/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

## Branch Workflow

1. Create feature branch: `git checkout -b feature/your-feature`
2. Make changes and commit
3. Push and create PR: `gh pr create`
4. Wait for manual review and merge

## Configuration

Environment-specific configuration in `configs/`:

| File | Environment | Catalog | Compute |
|------|-------------|---------|---------|
| `dev.yml` | Development | `squad_dev` | Interactive cluster |
| `test.yml` | Testing | `squad_test` | Job cluster |
| `prod.yml` | Production | `squad` | Job cluster (optimized) |

## License

MIT
