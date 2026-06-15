# Architecture Documentation

## Overview

This project implements a medallion architecture (Bronze/Silver/Gold) for a financial transactions data platform using Databricks, Delta Lake, and Unity Catalog.

## Design Principles

### 1. Separation of Concerns

Each layer has a distinct responsibility:

- **Bronze**: Raw data preservation with minimal transformation
- **Silver**: Business logic, quality validation, dimensional modeling
- **Gold**: Pre-computed aggregations for analytics consumption

### 2. Immutability & Lineage

- Bronze layer is append-only to preserve raw data history
- All transformations are reproducible
- Full lineage tracking via metadata columns

### 3. Quality Gates

Data must pass quality checks before promotion to the next layer:

```
Bronze → Validation → Silver (valid) + Quarantine (invalid)
Silver → Aggregation → Gold
```

## Layer Details

### Bronze Layer

**Purpose**: Ingest and store raw data as-is from source systems.

**Tables**:
| Table | Source | Update Pattern |
|-------|--------|----------------|
| transactions_raw | CSV/JSON/API | Append |
| customers_raw | CSV/API | Append |
| accounts_raw | CSV/API | Append |
| merchants_raw | CSV/API | Append |

**Metadata Columns**:
- `_source_file`: Origin of the data
- `_batch_id`: Unique batch identifier
- `_ingestion_timestamp`: When data was ingested

**Key Decisions**:
- No schema enforcement (infer from source)
- Keep bad data for debugging
- Partition by ingestion date for efficient queries

### Silver Layer

**Purpose**: Clean, validate, and model data for analytical use.

#### Dimension Tables

| Table | SCD Type | Description |
|-------|----------|-------------|
| dim_customer | Type 2 | Customer master with history |
| dim_account | Type 2 | Account master with history |
| dim_merchant | Type 1 | Merchant master (overwrite) |
| dim_date | Static | Calendar dimension 2020-2030 |

**SCD Type 2 Implementation**:
```
- is_current: Boolean flag for active record
- valid_from: Start of validity period
- valid_to: End of validity period (null for current)
```

#### Fact Table

**fact_transactions**:
- Surrogate keys for dimension joins
- Typed and validated fields
- USD normalized amounts
- Audit timestamps

**Quality Validation Rules**:
1. Required fields not null (transaction_id, amount)
2. Amount >= 0 (except refunds)
3. Currency in valid set
4. Status in valid enum
5. Dates parseable and in range

#### Quarantine Table

Invalid records are stored with:
- Original record (as JSON or columns)
- `_rejection_reason`: Why rejected
- `_rejected_at`: When rejected
- `_batch_id`: Source batch

### Gold Layer

**Purpose**: Pre-computed analytics and KPIs for business consumption.

#### Analytics Tables

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| revenue_summary | Day/Channel/Type/Country | Revenue, count, avg |
| customer_analytics | Customer | LTV, segment, activity |
| merchant_analytics | Merchant | Revenue, tier, customers |
| channel_performance | Month/Channel | Trends, share |
| geographic_summary | Quarter/Country | Regional stats |

#### KPI Tables

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| daily_kpis | Day | Rolling avg, DoD change |
| monthly_kpis | Month | MoM, YoY comparisons |
| executive_summary | Period | High-level KPIs |

## Data Flow

```
Source Data
    │
    ▼
┌──────────────────────────────────────┐
│  Bronze Loaders                       │
│  - Add metadata                       │
│  - Write to Delta                     │
└──────────────────────────────────────┘
    │
    ▼
┌──────────────────────────────────────┐
│  Silver Transformers                  │
│  - Validate quality                   │
│  - Type casting                       │
│  - Surrogate keys                     │
│  - Currency normalization             │
│  - Dimension builds (SCD)             │
└──────────────────────────────────────┘
    │
    ├─── Valid records ───► fact_transactions
    │
    └─── Invalid records ──► quarantine
    │
    ▼
┌──────────────────────────────────────┐
│  Gold Builders                        │
│  - Aggregate metrics                  │
│  - Calculate KPIs                     │
│  - Build summaries                    │
└──────────────────────────────────────┘
    │
    ▼
Analytics / BI Tools
```

## Key Technical Decisions

### 1. Surrogate Keys

Using hash-based surrogate keys for dimension joins:
```python
transaction_key = abs(hash(transaction_id))
customer_key = abs(hash(customer_id))
date_key = YYYYMMDD integer format
```

**Rationale**: Deterministic, reproducible, no sequence management.

### 2. Currency Normalization

All amounts converted to USD at ingestion time using fixed rates.

**Rationale**: Simplifies analytics, consistent comparisons. Rates stored in code for reproducibility.

### 3. Incremental Processing

Silver transformer supports incremental processing via:
- Batch ID tracking
- Max ingestion timestamp watermark
- Delta MERGE for upserts

### 4. Quality Thresholds

Different thresholds by environment:
| Check | Dev | Test | Prod |
|-------|-----|------|------|
| Null rate | 10% | 5% | 2% |
| Duplicate rate | 5% | 1% | 0.5% |
| Freshness hours | 48 | 24 | 12 |

### 5. Testing Strategy

- **Unit tests**: Individual functions with mocked Spark
- **Integration tests**: Bronze→Silver→Gold flow
- **Data quality tests**: Business rules validation
- **Smoke tests**: End-to-end with real data

## Performance Considerations

### Partitioning

| Table | Partition Columns |
|-------|-------------------|
| transactions_raw | _ingestion_date |
| fact_transactions | date_key |
| quarantine | _rejected_date |
| revenue_summary | report_date |

### Optimization

- Z-ORDER on frequently filtered columns
- OPTIMIZE on regular schedule
- VACUUM with 7-day retention
- Statistics collection for query planning

## Monitoring

### Key Metrics

- Records processed per batch
- Quality check pass rates
- Quarantine rates by reason
- Pipeline execution duration
- Data freshness lag

### Alerting

- Quarantine rate > 5%
- Data freshness > 24 hours
- Pipeline failure
- Schema drift detected

## Future Enhancements

1. **Streaming ingestion**: Bronze layer from Kafka
2. **ML features**: Feature store integration
3. **Data contracts**: Schema registry enforcement
4. **Cost tracking**: Per-table compute usage
