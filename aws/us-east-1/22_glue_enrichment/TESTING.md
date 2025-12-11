# Bronze → Silver-core → Silver-sentiment Testing Guide

This document provides step-by-step instructions for validating the Bronze to Silver data pipeline.

## Prerequisites

1. Ensure all Terraform modules are applied:
   ```bash
   # Apply 22_glue_enrichment module
   cd aws/us-east-1/22_glue_enrichment
   terraform init
   terraform plan -var-file=../../prod.tfvars
   terraform apply -var-file=../../prod.tfvars
   
   # Apply 30_stepfunction module (with updated orchestration)
   cd ../30_stepfunction
   terraform init
   terraform plan -var-file=../../prod.tfvars
   terraform apply -var-file=../../prod.tfvars
   ```

2. Verify Bronze layer has data:
   ```bash
   # Check Bronze tables via AWS CLI or Athena
   aws glue get-table --database-name brandrep_bronze --name submissions
   aws glue get-table --database-name brandrep_bronze --name comments
   ```

3. Identify a valid `PROCESS_DATE` with existing Bronze data:
   ```sql
   -- Run in Athena
   SELECT DISTINCT partition_date 
   FROM brandrep_bronze.submissions 
   ORDER BY partition_date DESC 
   LIMIT 5;
   ```

## Test Scenarios

### Scenario 1: Standalone Silver-core Job Test

Test the Silver-core cleaning job in isolation.

**Step 1: Run the Glue job manually**

```bash
# Replace with your actual PROCESS_DATE (e.g., "10-12-2024")
PROCESS_DATE="DD-MM-YYYY"

aws glue start-job-run \
  --job-name "tf-prod-smartcomp-glue-enrichment-reddit-silver-core" \
  --arguments '{
    "--PROCESS_DATE":"'$PROCESS_DATE'"
  }'
```

**Step 2: Monitor job execution**

```bash
# Get the job run ID from the previous command output
JOB_RUN_ID="jr_xxxxx"

# Check job status
aws glue get-job-run \
  --job-name "tf-prod-smartcomp-glue-enrichment-reddit-silver-core" \
  --run-id "$JOB_RUN_ID" \
  --query 'JobRun.JobRunState'
```

**Step 3: Verify Silver-core tables**

```sql
-- Run in Athena
-- Check submissions_silver table
SELECT 
  COUNT(*) as total_submissions,
  SUM(CASE WHEN is_usable_for_sentiment THEN 1 ELSE 0 END) as usable_submissions,
  CAST(SUM(CASE WHEN is_usable_for_sentiment THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as usable_ratio
FROM brandrep_silver.submissions_silver
WHERE partition_date = 'DD-MM-YYYY';

-- Check comments_silver table
SELECT 
  COUNT(*) as total_comments,
  SUM(CASE WHEN is_usable_for_sentiment THEN 1 ELSE 0 END) as usable_comments,
  CAST(SUM(CASE WHEN is_usable_for_sentiment THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as usable_ratio
FROM brandrep_silver.comments_silver
WHERE partition_date = 'DD-MM-YYYY';
```

**Step 4: Validate data quality**

```sql
-- Verify no rows were dropped (compare with Bronze)
SELECT 
  'Bronze' as layer,
  COUNT(*) as submission_count
FROM brandrep_bronze.submissions
WHERE partition_date = 'DD-MM-YYYY'
UNION ALL
SELECT 
  'Silver-core' as layer,
  COUNT(*) as submission_count
FROM brandrep_silver.submissions_silver
WHERE partition_date = 'DD-MM-YYYY';

-- Sample flagged submissions (unusable for sentiment)
SELECT 
  id, 
  title, 
  selftext, 
  is_usable_for_sentiment
FROM brandrep_silver.submissions_silver
WHERE partition_date = 'DD-MM-YYYY'
  AND is_usable_for_sentiment = false
LIMIT 10;

-- Sample flagged comments
SELECT 
  id, 
  body, 
  is_usable_for_sentiment
FROM brandrep_silver.comments_silver
WHERE partition_date = 'DD-MM-YYYY'
  AND is_usable_for_sentiment = false
LIMIT 10;
```

**Step 5: Check CloudWatch metrics**

```bash
# View Silver-core metrics
aws cloudwatch get-metric-statistics \
  --namespace RedditSilverCore \
  --metric-name SubmissionsUsableRatio \
  --dimensions Name=Environment,Value=prod Name=JobName,Value=tf-prod-smartcomp-glue-enrichment-reddit-silver-core \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 3600 \
  --statistics Average
```

### Scenario 2: Standalone Silver-sentiment Job Test

Test the sentiment analysis job (requires Silver-core to be completed first).

**Step 1: Run the Glue job manually**

```bash
PROCESS_DATE="DD-MM-YYYY"

aws glue start-job-run \
  --job-name "tf-prod-smartcomp-glue-enrichment-reddit-silver-sentiment" \
  --arguments '{
    "--PROCESS_DATE":"'$PROCESS_DATE'"
  }'
```

**Step 2: Monitor and verify sentiment results**

```sql
-- Check sentiment_analysis table
SELECT 
  COUNT(*) as total_sentiment_records,
  COUNT(DISTINCT comment_id) as unique_comments_analyzed,
  COUNT(DISTINCT brand_name) as brands_mentioned,
  AVG(sentiment_score) as avg_sentiment_score
FROM brandrep_silver.sentiment_analysis
WHERE partition_date = 'DD-MM-YYYY';

-- Sample sentiment results
SELECT 
  comment_id,
  brand_name,
  category,
  scope,
  sentiment_score,
  analyzed_at
FROM brandrep_silver.sentiment_analysis
WHERE partition_date = 'DD-MM-YYYY'
LIMIT 20;

-- Verify only usable comments were analyzed
SELECT 
  c.id as comment_id,
  c.is_usable_for_sentiment,
  s.comment_id as analyzed_comment_id
FROM brandrep_silver.comments_silver c
LEFT JOIN brandrep_silver.sentiment_analysis s 
  ON c.id = s.comment_id 
  AND c.partition_date = s.partition_date
WHERE c.partition_date = 'DD-MM-YYYY'
  AND c.is_usable_for_sentiment = false
  AND s.comment_id IS NOT NULL;
-- This query should return 0 rows (no unusable comments were analyzed)
```

**Step 3: Check Gemini API integration**

```sql
-- Check for API failures or missing results
SELECT 
  c.id as comment_id,
  c.body,
  c.is_usable_for_sentiment,
  s.comment_id as has_sentiment
FROM brandrep_silver.comments_silver c
LEFT JOIN brandrep_silver.sentiment_analysis s 
  ON c.id = s.comment_id 
  AND c.partition_date = s.partition_date
WHERE c.partition_date = 'DD-MM-YYYY'
  AND c.is_usable_for_sentiment = true
  AND s.comment_id IS NULL
LIMIT 50;
-- Some missing results are expected due to API timeouts or no brand mentions
```

### Scenario 3: End-to-End Step Functions Test

Test the complete orchestrated pipeline.

**Step 1: Trigger Step Functions execution**

```bash
# Option 1: Trigger via EventBridge (scheduled)
# Wait for the next scheduled execution

# Option 2: Manually start execution
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:tf-reddit-listing-ingestion" \
  --input '{}'
```

**Step 2: Monitor execution**

```bash
# Get execution ARN from previous command
EXECUTION_ARN="arn:aws:states:us-east-1:ACCOUNT_ID:execution:..."

# Check execution status
aws stepfunctions describe-execution \
  --execution-arn "$EXECUTION_ARN" \
  --query 'status'

# View execution history
aws stepfunctions get-execution-history \
  --execution-arn "$EXECUTION_ARN" \
  --max-items 100
```

**Step 3: Verify complete pipeline**

```sql
-- End-to-end validation query
WITH pipeline_summary AS (
  SELECT 
    'Bronze' as layer,
    COUNT(*) as submission_count,
    COUNT(DISTINCT id) as unique_submissions
  FROM brandrep_bronze.submissions
  WHERE partition_date = 'DD-MM-YYYY'
  
  UNION ALL
  
  SELECT 
    'Silver-core' as layer,
    COUNT(*) as submission_count,
    COUNT(DISTINCT id) as unique_submissions
  FROM brandrep_silver.submissions_silver
  WHERE partition_date = 'DD-MM-YYYY'
)
SELECT * FROM pipeline_summary;

-- Comments flow
WITH comments_flow AS (
  SELECT 
    'Bronze' as layer,
    COUNT(*) as comment_count,
    NULL as usable_count,
    NULL as analyzed_count
  FROM brandrep_bronze.comments
  WHERE partition_date = 'DD-MM-YYYY'
  
  UNION ALL
  
  SELECT 
    'Silver-core' as layer,
    COUNT(*) as comment_count,
    SUM(CASE WHEN is_usable_for_sentiment THEN 1 ELSE 0 END) as usable_count,
    NULL as analyzed_count
  FROM brandrep_silver.comments_silver
  WHERE partition_date = 'DD-MM-YYYY'
  
  UNION ALL
  
  SELECT 
    'Silver-sentiment' as layer,
    NULL as comment_count,
    NULL as usable_count,
    COUNT(DISTINCT comment_id) as analyzed_count
  FROM brandrep_silver.sentiment_analysis
  WHERE partition_date = 'DD-MM-YYYY'
)
SELECT * FROM comments_flow;
```

## Validation Checklist

### Data Integrity

- [ ] No rows dropped between Bronze and Silver-core (counts match)
- [ ] All Silver-core rows have `is_usable_for_sentiment` flag (no NULLs)
- [ ] Deleted/removed submissions are flagged as unusable
- [ ] Deleted/removed comments are flagged as unusable
- [ ] Short submissions/comments are flagged according to thresholds

### Sentiment Analysis

- [ ] Only usable comments were sent to Gemini API
- [ ] Sentiment results contain valid brand names and categories
- [ ] Normalized brands match reference data (`dim_products_ref`)
- [ ] Sentiment scores are between 0 and 1
- [ ] All sentiment records have valid `analyzed_at` timestamps

### Metrics

- [ ] CloudWatch metrics are published for Silver-core job
- [ ] CloudWatch metrics are published for Silver-sentiment job
- [ ] Usability ratios are reasonable (not 0% or 100%)
- [ ] Job durations are within acceptable range

### Orchestration

- [ ] Step Function execution completes successfully
- [ ] All three states execute in order: Bronze → Silver-core → Silver-sentiment
- [ ] `PROCESS_DATE` is passed correctly through all states
- [ ] IAM permissions are sufficient for all operations

## Troubleshooting

### Common Issues

**Issue**: Silver-core job fails with "Table not found"
- **Solution**: Ensure Bronze Glue job ran successfully and created Bronze tables

**Issue**: Sentiment job has no results
- **Solution**: Check that Silver-core flagged some comments as usable (not all filtered out)

**Issue**: Gemini API errors
- **Solution**: Verify Secrets Manager secret ARN is correct and contains valid API key

**Issue**: Step Function fails on Silver-core state
- **Solution**: Check IAM policy includes permissions for `glue:StartJobRun` on Silver-core job ARN

**Issue**: Low usability ratios (< 10%)
- **Solution**: Review threshold configuration; may be too restrictive

## Performance Benchmarks

Expected performance for a typical partition (1,000 submissions, 50,000 comments):

| Job | Expected Duration | Worker Type | Workers |
|-----|------------------|-------------|---------|
| Silver-core | 5-10 minutes | G.1X | 2 |
| Silver-sentiment | 30-60 minutes* | G.1X | 2 |

*Depends heavily on Gemini API rate limits and number of usable comments

## Next Steps

After successful validation:

1. Enable scheduled Step Functions executions
2. Set up CloudWatch alarms for job failures
3. Create dashboards for monitoring usability ratios
4. Tune thresholds based on observed data quality
5. Consider scaling up workers for larger partitions

