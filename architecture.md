# Architecture

## Overview

This project implements a medallion architecture for financial transaction analytics using Databricks, PySpark, and Delta Lake.

## Medallion Layers

### Bronze Layer (Raw)

**Purpose:** Ingest raw source data with minimal transformation, preserving source fidelity.

**Characteristics:**
- Schema-on-read for raw data
- Ingestion metadata added (_ingestion_timestamp, _source_file, _batch_id)
- Partitioned by source system
- Supports replay and reprocessing
- Auto-compaction enabled

**Tables:**
| Table | Description | Partition Key |
|-------|-------------|---------------|
| `transactions_raw` | Raw transaction records | `_source_system` |
| `customers_raw` | Raw customer master data | `_source_system` |
| `accounts_raw` | Raw account data | `_source_system` |
| `merchants_raw` | Raw merchant data | `_source_system` |

### Silver Layer (Cleansed)

**Purpose:** Validate, cleanse, deduplicate, and conform data for analytics.

**Characteristics:**
- Enforced schema with proper data types
- Deduplication by business key
- Null handling and data quality checks
- Referential integrity validation
- Invalid records quarantined
- SCD Type 2 for slowly changing dimensions

**Tables:**

*Dimensions:*
| Table | Description | Key |
|-------|-------------|-----|
| `dim_customer` | Customer dimension | `customer_key` |
| `dim_account` | Account dimension | `account_key` |
| `dim_merchant` | Merchant dimension | `merchant_key` |
| `dim_date` | Date dimension | `date_key` |

*Facts:*
| Table | Description | Grain |
|-------|-------------|-------|
| `fact_transactions` | Cleansed transactions | One row per transaction |

*Quarantine:*
| Table | Description |
|-------|-------------|
| `quarantine_transactions` | Invalid/rejected transactions |
| `quarantine_orphan_keys` | Records with invalid foreign keys |

### Gold Layer (Analytics)

**Purpose:** Business-ready aggregations and KPIs for reporting.

**Characteristics:**
- Pre-aggregated for query performance
- Optimized partitioning for BI tools
- Includes calculated metrics
- Liquid clustering where supported

**Tables:**
| Table | Description | Refresh |
|-------|-------------|---------|
| `daily_transaction_kpis` | Daily KPI summary | Daily |
| `monthly_financial_summary` | Monthly aggregations | Daily |
| `merchant_performance` | Merchant analytics | Daily |
| `customer_spend_summary` | Customer analytics | Daily |
| `channel_country_analytics` | Channel/country breakdown | Daily |

## Data Flow

```
┌─────────────┐
│ Source Files│
│ (CSV/JSON)  │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐ │
│  │transactions_raw│  │ customers_raw  │  │ merchants_raw  │ │
│  └────────────────┘  └────────────────┘  └────────────────┘ │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐ │
│  │fact_transactions│ │  dim_customer  │  │  dim_merchant  │ │
│  └────────────────┘  └────────────────┘  └────────────────┘ │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              quarantine_transactions                   │  │
│  └───────────────────────────────────────────────────────┘  │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                           │
│  ┌──────────────────┐  ┌──────────────────────────────────┐ │
│  │daily_txn_kpis    │  │  monthly_financial_summary       │ │
│  └──────────────────┘  └──────────────────────────────────┘ │
│  ┌──────────────────┐  ┌──────────────────────────────────┐ │
│  │merchant_perf     │  │  customer_spend_summary          │ │
│  └──────────────────┘  └──────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Unity Catalog Namespace

**Decision:** Use `squad` catalog with `bronze`, `silver`, `gold` schemas.

**Rationale:**
- Clear logical separation by data quality layer
- Enables fine-grained access control
- Supports data lineage tracking

**Fallback:** Hive databases `bronze`, `silver`, `gold` if Unity Catalog unavailable.

### 2. Delta Lake Features

**Decision:** Use Delta Lake with auto-optimize and compaction.

**Rationale:**
- ACID transactions for reliability
- Time travel for auditability
- Auto-compaction prevents small file problem
- Schema evolution support

### 3. Partitioning Strategy

| Layer | Partition Strategy | Reason |
|-------|-------------------|--------|
| Bronze | `_source_system` | Isolate source systems |
| Silver | `fiscal_year`, `fiscal_month` | Time-based queries |
| Gold | `fiscal_year` | Reporting periods |

### 4. Data Quality Approach

**Decision:** Quarantine invalid records rather than fail pipeline.

**Rationale:**
- Pipeline resilience
- Bad data visibility
- Enables remediation workflow
- Audit trail for data issues

### 5. Idempotency

**Decision:** Use MERGE for upserts with business keys.

**Rationale:**
- Safe reruns
- Late-arriving data handling
- Consistent state after failures

## Performance Optimizations

1. **Z-Ordering:** On frequently filtered columns (customer_id, transaction_date)
2. **Liquid Clustering:** For Gold tables when supported
3. **Broadcast Joins:** For small dimension tables
4. **Partition Pruning:** Date-based partitions align with query patterns
5. **Auto-Compaction:** Prevents small file proliferation

## Security Model

| Layer | Access Pattern |
|-------|----------------|
| Bronze | Data engineers only |
| Silver | Data engineers, analysts (read) |
| Gold | Analysts, BI tools, applications |

## Monitoring

- Row counts logged at each layer
- Quarantine counts tracked
- Processing duration metrics
- Data freshness checks
