# Runbook

## Daily Operations

### 1. Pipeline Execution

#### Standard Daily Run

```bash
# Deploy latest code
databricks bundle deploy -t prod

# Run Bronze ingestion
databricks bundle run 01_bronze_ingestion -t prod

# Run Silver transformation
databricks bundle run 02_silver_transforms -t prod

# Run Gold analytics
databricks bundle run 03_gold_analytics -t prod

# Run validation
databricks bundle run 99_validation_and_smoke_tests -t prod
```

#### Monitoring Run Status

```bash
# Check job status
databricks jobs list --output JSON | jq '.jobs[] | select(.settings.name | contains("squad"))'

# Get run history
databricks runs list --job-id <JOB_ID> --limit 10
```

---

## Troubleshooting

### Issue: High Quarantine Rate

**Symptoms:**
- Quarantine table growing rapidly
- Quality validation failing

**Investigation:**

```sql
-- Check rejection reasons
SELECT _rejection_reason, COUNT(*) as count
FROM squad.silver.quarantine
WHERE _rejected_at >= current_date() - 1
GROUP BY _rejection_reason
ORDER BY count DESC;

-- Sample rejected records
SELECT *
FROM squad.silver.quarantine
WHERE _rejection_reason = 'null_transaction_id'
LIMIT 10;
```

**Resolution:**
1. Identify source system issue
2. Fix upstream data
3. Reprocess batch:

```python
from src.silver.quarantine import QuarantineHandler

handler = QuarantineHandler(spark, config)
records = handler.reprocess_quarantine(batch_id="batch_XXXXXXXX")
# Process records through Silver transformer
```

---

### Issue: Data Freshness Alert

**Symptoms:**
- Data older than expected
- Freshness check failing

**Investigation:**

```sql
-- Check latest ingestion
SELECT MAX(_ingestion_timestamp) as latest
FROM squad.bronze.transactions_raw;

-- Check processing lag
SELECT MAX(_created_timestamp) as latest
FROM squad.silver.fact_transactions;
```

**Resolution:**
1. Check source system connectivity
2. Verify job schedules
3. Check for failed runs:

```bash
databricks runs list --job-id <JOB_ID> --state FAILED
```

---

### Issue: Schema Drift

**Symptoms:**
- Job failures with schema mismatch
- New columns appearing

**Investigation:**

```python
# Compare schemas
bronze_schema = spark.table("squad.bronze.transactions_raw").schema
expected_schema = BronzeTransactionsSchema

# Find differences
for field in bronze_schema.fields:
    if field.name not in [f.name for f in expected_schema.fields]:
        print(f"New field: {field.name}")
```

**Resolution:**
1. If new field is expected, update schema definition
2. If unexpected, investigate source system
3. For breaking changes, coordinate with downstream

---

### Issue: Performance Degradation

**Symptoms:**
- Jobs taking longer than usual
- Timeout failures

**Investigation:**

```sql
-- Check table sizes
DESCRIBE DETAIL squad.silver.fact_transactions;

-- Check for small files
SELECT
    count(*) as file_count,
    sum(size)/1024/1024/1024 as size_gb,
    avg(size)/1024/1024 as avg_file_mb
FROM (
    DESCRIBE DETAIL squad.silver.fact_transactions
);
```

**Resolution:**

```sql
-- Optimize table
OPTIMIZE squad.silver.fact_transactions
ZORDER BY (customer_key, date_key);

-- Vacuum old files
VACUUM squad.silver.fact_transactions RETAIN 168 HOURS;

-- Update statistics
ANALYZE TABLE squad.silver.fact_transactions COMPUTE STATISTICS;
```

---

## Disaster Recovery

### Scenario: Corrupted Silver Table

```sql
-- Restore from time travel (Delta Lake)
RESTORE TABLE squad.silver.fact_transactions
TO TIMESTAMP AS OF '2024-01-15T00:00:00.000Z';

-- Or restore to version
RESTORE TABLE squad.silver.fact_transactions
TO VERSION AS OF 42;
```

### Scenario: Full Reprocessing Required

```bash
# 1. Truncate Silver and Gold tables
databricks bundle run truncate_silver_gold -t prod

# 2. Reprocess all Bronze data
databricks bundle run full_reprocess -t prod
```

### Scenario: Rollback Deployment

```bash
# Get previous bundle version
databricks bundle deployments list

# Deploy previous version
git checkout <previous-commit>
databricks bundle deploy -t prod
```

---

## Maintenance Tasks

### Weekly: Optimize Tables

```sql
-- Run weekly
OPTIMIZE squad.bronze.transactions_raw;
OPTIMIZE squad.silver.fact_transactions ZORDER BY (date_key);
OPTIMIZE squad.gold.revenue_summary;

-- Vacuum old versions
VACUUM squad.bronze.transactions_raw RETAIN 168 HOURS;
VACUUM squad.silver.fact_transactions RETAIN 168 HOURS;
```

### Monthly: Quarantine Cleanup

```sql
-- Archive old quarantine records
CREATE TABLE IF NOT EXISTS squad.silver.quarantine_archive AS
SELECT * FROM squad.silver.quarantine WHERE 1=0;

INSERT INTO squad.silver.quarantine_archive
SELECT * FROM squad.silver.quarantine
WHERE _rejected_at < current_date() - 30;

DELETE FROM squad.silver.quarantine
WHERE _rejected_at < current_date() - 30;
```

### Quarterly: Schema Review

1. Review Bronze schema for drift
2. Update dimension attributes if needed
3. Add new Gold metrics if requested
4. Update documentation

---

## Emergency Contacts

| Role | Contact | Escalation |
|------|---------|------------|
| On-call Engineer | Slack: #squad-oncall | PagerDuty |
| Data Platform Team | Slack: #data-platform | Email |
| Databricks Support | support@databricks.com | Phone |

---

## Useful Queries

### Record Counts Across Layers

```sql
SELECT 'bronze_transactions' as table_name, COUNT(*) as records
FROM squad.bronze.transactions_raw
UNION ALL
SELECT 'silver_fact' as table_name, COUNT(*) as records
FROM squad.silver.fact_transactions
UNION ALL
SELECT 'silver_quarantine' as table_name, COUNT(*) as records
FROM squad.silver.quarantine;
```

### Processing Metrics by Batch

```sql
SELECT
    _batch_id,
    COUNT(*) as total_records,
    MIN(_ingestion_timestamp) as batch_start,
    MAX(_ingestion_timestamp) as batch_end
FROM squad.bronze.transactions_raw
GROUP BY _batch_id
ORDER BY batch_start DESC
LIMIT 10;
```

### Quality Trend Analysis

```sql
SELECT
    DATE(_rejected_at) as date,
    _rejection_reason,
    COUNT(*) as rejections
FROM squad.silver.quarantine
WHERE _rejected_at >= current_date() - 7
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```
