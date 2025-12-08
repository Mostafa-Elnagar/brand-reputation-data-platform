## Market Opinion Data Platform

This repository contains the infrastructure and application code for ingesting Reddit data into an AWS-based data lake, with a scheduled ingestion pipeline, observability, and clear separation of stages using Terraform.

The current scope focuses on **Reddit ingestion into the landing and bronze layers**, with:
- A modular Python ingestion package (`modules/reddit_ingest`).
- An AWS Lambda function that performs **listing-based** ingestion from Reddit.
- A Step Functions state machine triggered by EventBridge on a **schedule**.
- Landing-zone JSONL data in S3, and **Bronze Iceberg tables** populated via AWS Glue.
- Run metadata and checkpoints stored in DynamoDB, plus metrics in CloudWatch.

---

## Repository Structure

- `aws/`
  - `us-east-1/10_s3/` – S3 data lake buckets:
    - Landing zone: raw Reddit data.
    - Augmented zone: (reserved for later processing stages).
  - `us-east-1/11_dynamodb/` – Shared DynamoDB tables:
    - `reddit_ingestion_checkpoints` table for listing-based checkpoints.
    - `lambda_run_logs` table for recording per-run metadata.
  - `us-east-1/20_lambda/` – Reddit ingestion Lambda:
    - Lambda function, IAM roles/policies.
    - Uses shared DynamoDB tables from `11_dynamodb` for checkpoints and run metadata.
    - Remote state references to `10_s3` (buckets) and `11_dynamodb` (tables).
  - `us-east-1/21_glue/` – AWS Glue metadata and Bronze ingestion:
    - Landing Glue database for Reddit raw data.
    - Bronze Glue database and Iceberg tables for submissions and comments.
    - Crawlers for landing-zone `submissions.jsonl` and `comments.jsonl`.
    - Glue job for **Bronze ingestion** from landing JSONL to Iceberg tables.
  - `us-east-1/30_stepfunction/` – Step Functions state machine:
    - Orchestrates listing-based ingestion using multiple Lambda invocations.
  - `us-east-1/40_eventbridge/` – EventBridge rule and target:
    - Triggers the Step Functions state machine on a schedule.
- `modules/`
  - `reddit_ingest/`
    - `reddit_client.py` – Creates a configured PRAW `Reddit` client from environment variables.
    - `fetchers.py` – Core fetcher utilities (e.g., `CommentFetcher` and retry helpers).
    - `simple_listing_fetcher.py` – `SimpleListingFetcher` for `/top` and `/controversial` listings.
    - `serializers.py` – Converts PRAW `Submission`/`Comment` objects into JSON-safe dicts.
    - `s3_storage.py` – `S3JsonlWriter` that writes JSONL records directly to S3.
    - `ingestion_service.py` – Orchestrates fetching, serialization, and writing.
    - `state_store.py` – DynamoDB-backed checkpoint store for listing-based ingestion.
    - `run_metadata.py` – Persists run metadata to DynamoDB and publishes CloudWatch metrics.
- `lambda/`
  - `ingestion/handler.py` – AWS Lambda handler wiring together the Reddit client, listing fetcher, ingestion service, checkpoint store, and run recorder.
- `scripts/`
  - `build_lambda.sh` – Builds the Lambda deployment ZIP using `lambda/ingestion/requirements.txt`.
- `tests/`
  - Pytest-based tests for `modules/reddit_ingest` (fetchers, listing fetcher, ingestion service).
- `run_reddit_backfill.sh`
  - Backfill driver that launches Step Functions executions over a historical time range.
- `build/`
  - Contains generated Lambda ZIP artifacts (not checked in, but used during deployment).

---

## Ingestion Architecture

### High-Level Flow

1. **Schedule (EventBridge + Step Functions)**
   - An EventBridge rule in `aws/us-east-1/40_eventbridge` triggers the `reddit_listing_ingestion` Step Functions state machine on a configured schedule (for example, `rate(1 hour)`).
   - The Step Function fans out into multiple Lambda invocations, each handling a single `(subreddit, sort_type)` listing ingestion.

2. **Lambda Handler (`lambda/ingestion/handler.py`)**
   - Reads configuration from environment variables:
     - `LANDING_BUCKET_NAME`, `LANDING_BUCKET_PREFIX`.
     - `REDDIT_SECRET_ARN` (Secrets Manager ARN).
     - `DEFAULT_SUBREDDIT`.
     - `ENVIRONMENT`.
     - `RUNS_TABLE_NAME`, `CHECKPOINTS_TABLE_NAME`.
     - `METRICS_NAMESPACE`.
   - Loads Reddit API credentials from Secrets Manager.
   - Validates the incoming event:
     - `subreddit` (default from `DEFAULT_SUBREDDIT` if omitted).
     - `sort_type` in `("top", "controversial")`.
     - Optional `time_filter`, `max_items`, and `include_comments`.
   - Builds an S3 prefix of the form:
     - `reddit/{sort_type}/subreddit={subreddit}/date=DD-MM-YYYY/`.
   - Wires together:
     - `RedditClientFactory` → PRAW `Reddit` client.
     - `SimpleListingFetcher` → listing-based submissions iterator.
     - `CommentFetcher` → comment expansion for each submission.
     - `S3JsonlWriter` → JSONL writers for submissions and comments.
     - `RedditIngestionService` → orchestrates the fetch/write flow.
     - `RedditCheckpointStore` → persists listing checkpoints.
     - `RedditIngestionRunRecorder` → records run metadata and metrics.

3. **Reddit Ingestion (`modules/reddit_ingest`)**
   - **Client**: `RedditClientFactory.create()` builds a PRAW client from env vars:
   - `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`.
   - **Fetchers**:
   - `SimpleListingFetcher.fetch_listing(subreddit_name, sort_type, time_filter, limit)` reads submissions from `/top` or `/controversial` for a subreddit, respecting a maximum item limit.
   - `CommentFetcher.iterate_comments(submission)` expands and flattens all comments for a submission.
   - **Serialization**:
   - `SubmissionSerializer.to_dict` and `CommentSerializer.to_dict` produce JSON-safe dicts, including useful fields like author, scores, `created_utc`, and awards.
   - Each record carries a stable Reddit **fullname identifier** (`fullname_id`, e.g. `t3_xxx` for submissions, `t1_xxx` for comments) and an **`updated_at`** unix timestamp representing when the ingestion run observed that object.
   - Downstream consumers should treat the S3 JSONL as an **append-only event stream** and perform **upserts keyed by `fullname_id`**, keeping the latest `updated_at` per id.
   - **Storage**:
   - `S3JsonlWriter` writes each serialized object as a JSON line into S3, with optional rotation and buffering:
       - S3 key pattern: `reddit/{sort_type}/subreddit={subreddit}/date=DD-MM-YYYY/submissions.jsonl` and `comments.jsonl`.
   - **Service**:
   - `RedditIngestionService.ingest_listing(subreddit_name, simple_fetcher, sort_type, time_filter="all", max_items=1000, include_comments=True)`:
       - Uses `SimpleListingFetcher` to stream submissions from a single listing endpoint.
       - Optionally fetches and writes all comments for each submission.
       - Returns a `RedditIngestionResult` with:
         - `subreddit`, `submissions_count`, `comments_count`, `s3_prefix`, `sort_type`, `time_filter`.
       - Logs start/end of each ingestion run (subreddit, sort type, counts).

4. **State Management (`modules/reddit_ingest/state_store.py`)**
   - `RedditCheckpointStore` uses the shared `reddit_ingestion_checkpoints` DynamoDB table from `11_dynamodb` to persist listing checkpoints:
     - Keys:
       - `pk = "ENV#{environment}#SUBREDDIT#{subreddit}"`.
       - `sk = "CHECKPOINT#{checkpoint_type}#{sort_type}#{time_filter}"` for listing checkpoints.
     - Attributes:
       - `LastToken`, `LastTimestamp`, `SortType`, `TimeFilter`, `ItemsFetched`, `S3Location`.
   - Checkpoints can be used to understand which listings were already ingested, how many items were fetched, and where they were written in S3.

5. **Run Metadata and Metrics (`modules/reddit_ingest/run_metadata.py`)**
   - `RedditIngestionRunRecorder`:
     - Writes a record per ingestion run into the generic `lambda_run_logs` DynamoDB table from `11_dynamodb`:
       - `pk = "ENV#{environment}#SUBREDDIT#{subreddit}"`.
       - `sk = "RUN#{run_timestamp}#{sort_type}"`.
       - Attributes include `SubmissionsCount`, `CommentsCount`, `Mode`, `SortType`, `TimeFilter`, `S3Prefix`, `IngestedAtTs`.
     - Publishes CloudWatch metrics for each run:
       - Namespace: `RedditIngestion`.
       - Metrics:
         - `SubmissionsFetched` (Count).
         - `CommentsFetched` (Count).
       - Dimensions: `Environment`, `Subreddit`, `SortType`.
   - All operations log successes and failures via the module logger.

---

## Terraform Stages and Modules

### `10_s3`

Located at `aws/us-east-1/10_s3/`:

- Provisions S3 buckets using `terraform-aws-modules/s3-bucket/aws`:
  - **Landing zone bucket**: `${local.name_prefix}-lz`
  - **Augmented zone bucket**: `${local.name_prefix}-augmented`
- Buckets enforce:
  - Blocked public access.
  - Server-side encryption (SSE-S3 or KMS).
- Outputs:
  - `s3_landing_zone_bucket_id`, `s3_landing_zone_bucket_arn`
  - `s3_augmented_zone_bucket_id`, `s3_augmented_zone_bucket_arn`

The Lambda stack (`20_lambda`) reads these outputs via `terraform_remote_state`.

### `11_dynamodb`

Located at `aws/us-east-1/11_dynamodb/`:

- Provisions shared DynamoDB tables:
  - `reddit_ingestion_checkpoints` – listing-based checkpoint store.
  - `lambda_run_logs` – generic Lambda run logs.
- Tables use:
  - Partition key: `pk` (string).
  - Sort key: `sk` (string).
- Outputs:
  - Table names and ARNs for use in other stacks.

### `20_lambda`

Located at `aws/us-east-1/20_lambda/`:

- **Backend**: Uses the same S3 backend as other stages with key `20_lambda/terraform.tfstate`.
- **Provider**: Standard AWS provider configured via variables.
- **Data sources**:
  - `terraform_remote_state.s3` – fetch S3 bucket names/ARNs from `10_s3`.
  - `terraform_remote_state.dynamodb` – fetch DynamoDB table names/ARNs from `11_dynamodb`.
  - `aws_caller_identity.current`, `aws_region.current`.
- **Locals**:
  - `name_prefix` for consistent Lambda-related naming.
  - `landing_bucket_name`, `landing_bucket_arn`, `reddit_prefix = "reddit/"`.
  - `checkpoints_table_name`, `lambda_run_logs_table_name` derived from `11_dynamodb` remote state.
- **Resources**:
  - `aws_lambda_function.reddit_ingestion`:
    - Handler: `handler.lambda_handler`.
    - Runtime: `python3.11`.
    - Deployed from a ZIP artifact built under `build/reddit_ingestion_lambda/`.
    - Environment variables:
      - `LANDING_BUCKET_NAME`, `LANDING_BUCKET_PREFIX`.
      - `REDDIT_SECRET_ARN` (Secrets Manager ARN).
      - `DEFAULT_SUBREDDIT`.
      - `ENVIRONMENT`.
      - `RUNS_TABLE_NAME` (DynamoDB run logs table).
      - `CHECKPOINTS_TABLE_NAME` (DynamoDB checkpoints table).
      - `METRICS_NAMESPACE` (CloudWatch namespace).
  - IAM resources:
    - `aws_iam_role.reddit_ingestion_lambda` with `lambda.amazonaws.com` trust.
    - `aws_iam_policy.reddit_ingestion_inline` attached to the role:
      - CloudWatch Logs (log groups/streams/events).
      - S3 `PutObject` / `AbortMultipartUpload` to the landing bucket under `reddit/`.
      - Secrets Manager `GetSecretValue` for the Reddit credentials secret.
      - DynamoDB `PutItem` / `GetItem` on the checkpoints and run logs tables.
      - CloudWatch `PutMetricData` for publishing ingestion metrics.

### `21_glue`

Located at `aws/us-east-1/21_glue/`:

- **Backend**: Uses the same S3 backend with key `21_glue/terraform.tfstate`.
- **Resources**:
  - Landing Glue database for Reddit raw data.
  - Bronze Glue database with Iceberg tables:
    - `submissions` and `comments` with explicit schemas.
  - Crawlers over the landing bucket under the `reddit/` prefix:
    - One for `submissions.jsonl`.
    - One for `comments.jsonl`.
  - S3 object for `scripts/reddit_bronze_ingestion.py`.
  - Glue job `reddit_bronze_ingestion`:
    - Reads landing JSONL files.
    - Adds audit columns (`ingestion_timestamp`, `source_file`, `source_sort_type`, `partition_date`).
    - Writes into Bronze Iceberg tables using MERGE semantics.

### `30_stepfunction`

Located at `aws/us-east-1/30_stepfunction/`:

- **Backend**: Uses the same S3 backend with key `30_stepfunction/terraform.tfstate`.
- **Data sources**:
  - `terraform_remote_state.lambda` – fetch Lambda ARN from `20_lambda`.
- **Resources**:
  - IAM role for Step Functions with permission to invoke the ingestion Lambda.
  - `aws_sfn_state_machine.reddit_listing_ingestion`:
    - Definition loaded from `listing_ingestion_state_machine.asl.json`.
    - Uses a templated list of invocations (subreddit + sort_type combinations).

### `40_eventbridge`

Located at `aws/us-east-1/40_eventbridge/`:

- **Backend**: Uses the same S3 backend with key `40_eventbridge/terraform.tfstate`.
- **Data sources**:
  - `terraform_remote_state.stepfunction` – fetch Step Function ARN from `30_stepfunction`.
- **Resources**:
  - `aws_cloudwatch_event_rule.reddit_ingestion_schedule`:
    - `schedule_expression` (for example `rate(1 hour)`).
  - `aws_cloudwatch_event_target.reddit_ingestion_stepfunction`:
    - Targets the `reddit_listing_ingestion` Step Function.
  - IAM role for EventBridge to start Step Function executions.

---

## Configuration

### Global variables (`aws/prod.tfvars`)

Typical contents:

```hcl
prefix      = "tf-"
name        = "smartphone-compitition"
owner       = "INFRA_TEAM"
environment = "prod"
region      = "us-east-1"
vpc_cidr    = "10.0.0.0/16"
ManagedBy   = "TERRAFORM"

reddit_secret_arn               = "arn:aws:secretsmanager:us-east-1:ACCOUNT_ID:secret:reddit_api-XXXX"
reddit_ingestion_lambda_package = "/absolute/path/to/build/reddit_ingestion_lambda/reddit_ingestion_lambda.zip"
```

The Lambda package path is set after you build the ZIP locally (see below).

### Reddit API Credentials (Secrets Manager)

The secret referenced by `reddit_secret_arn` must be a JSON document:

```json
{
  "REDDIT_CLIENT_ID": "your-client-id",
  "REDDIT_CLIENT_SECRET": "your-client-secret",
  "REDDIT_USER_AGENT": "your-app-name"
}
```

---

## Building and Deploying the Ingestion Lambda

From the repository root:

```bash
# 1. Build the Lambda artifact
./scripts/build_lambda.sh
```

This script:

- Installs dependencies from `lambda/ingestion/requirements.txt` into a build directory.
- Copies the `lambda/ingestion/handler.py` and `modules/` package.
- Produces `build/reddit_ingestion_lambda/reddit_ingestion_lambda.zip`.

Update `aws/prod.tfvars` to point `reddit_ingestion_lambda_package` to the absolute path of the ZIP if needed.

Then deploy:

```bash
cd ../../aws/us-east-1/10_s3
terraform init
terraform apply -var-file=../../prod.tfvars

cd ../11_dynamodb
terraform init
terraform apply -var-file=../../prod.tfvars

cd ../20_lambda
terraform init
terraform apply -var-file=../../prod.tfvars

cd ../21_glue
terraform init
terraform apply -var-file=../../prod.tfvars

cd ../30_stepfunction
terraform init
terraform apply -var-file=../../prod.tfvars

cd ../40_eventbridge
terraform init
terraform apply -var-file=../../prod.tfvars
```

---

## Testing the Ingestion Pipeline

### Manual listing-based run

```bash
aws lambda invoke \
  --function-name tf-prod-smartcomp-lambda-reddit-ingestion \
  --payload '{"subreddit":"smartphones","sort_type":"top","time_filter":"all","max_items":200,"include_comments":true}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/reddit_ingestion_listing.json

cat /tmp/reddit_ingestion_listing.json
```

Check:
- DynamoDB: recent items in `lambda_run_logs` table.
- DynamoDB: checkpoints in `reddit_ingestion_checkpoints` table.
- S3: new JSONL objects under `reddit/{subreddit}/...`.
- CloudWatch Logs: INFO logs for window selection, ingestion, recording, and metrics.

## Logging & Observability

- All core components (`ingestion_service`, `state_store`, `run_metadata`, and the Lambda handler) use Python’s `logging` module:
  - High-level events at `INFO`.
  - Warnings for abnormal-but-non-fatal conditions (e.g., invalid SSM values).
  - Errors for unexpected AWS API or runtime failures.
- Metrics:
  - `SubmissionsFetched` and `CommentsFetched` per run in CloudWatch under the `RedditIngestion` namespace, with `Environment`, `Subreddit`, and `SortType` dimensions.
- Run history:
  - DynamoDB `lambda_run_logs` table allows you to query history per subreddit and environment, order by `sk` for time ordering, and audit ingestion coverage.

This design keeps the ingestion pipeline modular, observable, and aligned with a staged Terraform layout, making it easier to extend later (e.g., sentiment analysis, augmented zone loading) without changing the existing contracts. 


