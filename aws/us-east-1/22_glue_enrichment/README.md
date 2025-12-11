# Silver Layer Glue Enrichment Module

This Terraform module provisions the infrastructure for the Silver layer of the data lakehouse, including:

1. **Silver-core tables**: Cleaned versions of Bronze tables with data quality flags
2. **Silver-sentiment tables**: Enriched data with sentiment analysis from Gemini API
3. **Reference data tables**: Dimension tables for product information

## Architecture

The module creates two main Glue jobs:

### 1. reddit_silver_core (Bronze → Silver-core)

This job reads from Bronze Iceberg tables and creates cleaned Silver-core tables with the following transformations:

- **Submissions**: Selects relevant columns and computes `is_usable_for_sentiment` flag
- **Comments**: Selects relevant columns and computes `is_usable_for_sentiment` flag
- **No deletions**: All rows are preserved to maintain tree structure and lineage
- **Metrics**: Emits CloudWatch metrics for data quality monitoring

#### Data Quality Flags

The `is_usable_for_sentiment` flag is computed based on configurable thresholds:

**Submissions** are flagged as unusable if:
- `selftext` is `[deleted]`, `[removed]`, or empty
- Combined `title + selftext` is below minimum character/word thresholds

**Comments** are flagged as unusable if:
- `body` is `[deleted]`, `[removed]`, or empty
- `body` is below minimum character/word thresholds

### 2. reddit_silver_sentiment (Silver-core → Silver-sentiment)

This job performs targeted sentiment analysis using the Gemini API:

- Reads from Silver-core tables
- Filters to only `is_usable_for_sentiment = true` comments
- Enriches context from submission title/selftext
- Calls Gemini API for sentiment analysis
- Normalizes brand/model names against reference data
- Writes results to `sentiment_analysis` Iceberg table

## Configuration Variables

### Required Variables

- `environment`: Environment name (e.g., `prod`, `dev`)
- `gemini_api_key_secret_arn`: ARN of the Secrets Manager secret containing the Gemini API key

### Optional Threshold Variables

These variables control the data quality thresholds for the `is_usable_for_sentiment` flag:

| Variable | Description | Default |
|----------|-------------|---------|
| `min_submission_len_chars` | Minimum character length for submissions | `10` |
| `min_submission_min_words` | Minimum word count for submissions | `2` |
| `min_comment_len_chars` | Minimum character length for comments | `30` |
| `min_comment_min_words` | Minimum word count for comments | `5` |

### Tuning Thresholds

To customize thresholds, add them to your `prod.tfvars`:

```hcl
# Silver-core cleaning thresholds
min_submission_len_chars  = 20
min_submission_min_words  = 3
min_comment_len_chars     = 50
min_comment_min_words     = 10
```

**Guidelines for threshold tuning:**

- **Lower thresholds** (current defaults): More permissive, includes more content for analysis
  - Pros: Preserves more data, captures short but meaningful comments
  - Cons: May include noise, low-quality or spam content

- **Higher thresholds**: More restrictive, focuses on higher-quality content
  - Pros: Better signal-to-noise ratio, potentially more accurate sentiment
  - Cons: May exclude valid short comments, reduces sample size

Monitor the CloudWatch metrics (`SubmissionsUsableRatio`, `CommentsUsableRatio`) to assess the impact of threshold changes.

## Iceberg Tables

### Silver-core Tables

- `brandrep_silver.submissions_silver`: Cleaned submissions with `is_usable_for_sentiment` flag
- `brandrep_silver.comments_silver`: Cleaned comments with `is_usable_for_sentiment` flag

### Enrichment Tables

- `brandrep_silver.sentiment_analysis`: Sentiment analysis results from Gemini
- `brandrep_silver.dim_products_ref`: Product reference data (brands, models, price segments)

## Monitoring

Both Glue jobs emit CloudWatch metrics to their respective namespaces:

### Silver-core Metrics (Namespace: `RedditSilverCore`)

- `SubmissionsTotal`: Total submissions processed
- `SubmissionsUsableForSentiment`: Submissions flagged as usable
- `SubmissionsUsableRatio`: Ratio of usable submissions
- `CommentsTotal`: Total comments processed
- `CommentsUsableForSentiment`: Comments flagged as usable
- `CommentsUsableRatio`: Ratio of usable comments
- `JobDurationSeconds`: Job execution time

### Silver-sentiment Metrics (Namespace: `RedditSilverSentiment`)

- `CommentsAnalyzedByGemini`: Comments sent to Gemini API
- `JobDurationSeconds`: Job execution time

## Dependencies

This module depends on:

- `10_s3`: S3 buckets for data lake
- `11_dynamodb`: DynamoDB for state tracking
- `21_glue`: Bronze layer Glue infrastructure

## Usage

```hcl
module "glue_enrichment" {
  source = "./22_glue_enrichment"
  
  environment               = var.environment
  gemini_api_key_secret_arn = var.gemini_api_key_secret_arn
  
  # Optional: Customize thresholds
  min_submission_len_chars  = 15
  min_comment_len_chars     = 40
}
```

