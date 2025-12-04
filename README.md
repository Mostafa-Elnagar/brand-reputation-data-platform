## Market Opinion Data Platform

This repository contains the infrastructure and application code for ingesting Reddit data into an AWS-based data lake, with an hourly incremental ingestion pipeline, observability, and clear separation of stages using Terraform.

The current scope focuses on **Reddit ingestion into the landing zone S3 bucket**, with:
- A modular Python ingestion package (`modules/reddit_ingest`).
- An AWS Lambda function that runs on a **hourly schedule**.
- Incremental, **time-window based** loading with safe backfill support.
- Run metadata and metrics stored in DynamoDB and CloudWatch.

---

## Repository Structure

- `aws/`
  - `us-east-1/10_s3/` – S3 data lake buckets:
    - Landing zone: raw Reddit data.
    - Augmented zone: (reserved for later processing stages).
  - `us-east-1/20_lambda/` – Reddit ingestion compute and glue:
    - Lambda function, IAM roles/policies.
    - EventBridge schedule.
    - DynamoDB table for ingestion run metadata.
    - Remote state references to `10_s3`.
- `modules/`
  - `reddit_ingest/`
    - `reddit_client.py` – Creates a configured PRAW `Reddit` client from environment variables.
    - `fetchers.py` – `SubmissionFetcher` and `CommentFetcher` with retry logic.
    - `serializers.py` – Converts PRAW `Submission`/`Comment` objects into JSON-safe dicts.
    - `storage.py` – Filesystem JSONL writer (used for local flows).
    - `s3_storage.py` – `S3JsonlWriter` that writes JSONL records directly to S3.
    - `ingestion_service.py` – Orchestrates fetching, serialization, and writing.
    - `state_store.py` – Manages ingestion watermarks in SSM Parameter Store.
    - `run_metadata.py` – Persists run metadata to DynamoDB and publishes CloudWatch metrics.
- `lambda/`
  - `ingestion/handler.py` – AWS Lambda handler wiring together the ingestion service, state store, and run recorder.
- `build/`
  - Contains generated Lambda ZIP artifacts (not checked in, but used during deployment).

---

## Ingestion Architecture

### High-Level Flow

1. **Schedule (EventBridge)**
   - An EventBridge rule in `20_lambda` triggers the ingestion Lambda on a **`rate(1 hour)`** schedule.

2. **Lambda Handler (`lambda/ingestion/handler.py`)**
   - Reads configuration from environment variables.
   - Loads Reddit API credentials from Secrets Manager.
   - Resolves one or more **hourly ingestion windows**:
     - Uses `RedditIngestionStateStore` with SSM parameter path:
       - `/reddit-ingest/{ENVIRONMENT}/{subreddit}/last_run_ts`
     - Interprets the watermark as `last_processed_hour_end_ts`.
     - For incremental runs:
       - If no watermark (first run): process the **previous full hour**.
       - If watermark exists: process up to `MAX_HOURLY_WINDOWS_PER_RUN` contiguous hours after the watermark.
     - For backfill runs:
       - When `event.mode == "backfill"` or explicit `start_ts` / `end_ts` is provided:
         - Splits the range into hourly windows and processes each, but **does not** change the watermark.

3. **Reddit Ingestion (`modules/reddit_ingest`)**
   - **Client**: `RedditClientFactory.create()` builds a PRAW client from env vars:
     - `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, `REDDIT_USER_AGENT`.
   - **Fetchers**:
     - `SubmissionFetcher.iterate_window(subreddit_name, start_ts, end_ts)` reads submissions in a created_utc window.
     - `CommentFetcher.iterate_comments(submission)` expands and flattens all comments for a submission.
   - **Serialization**:
     - `SubmissionSerializer.to_dict` and `CommentSerializer.to_dict` produce JSON-safe dicts, including useful fields like author, scores, created_utc, and awards.
   - **Storage**:
     - `S3JsonlWriter` writes each serialized object as a JSON line into S3, with optional rotation and buffering:
       - S3 key pattern: `reddit/{subreddit}/{YYYY-MM-DD/HHMMSS}/submissions.jsonl` and `comments.jsonl`.
   - **Service**:
     - `RedditIngestionService.ingest_window(subreddit_name, start_ts, end_ts, include_comments=True)`:
       - Fetches all submissions in the window.
       - Optionally fetches and writes all comments for each submission.
       - Returns a `RedditIngestionResult` with:
         - `subreddit`, `submissions_count`, `comments_count`, `s3_prefix`, `window_start_ts`, `window_end_ts`.
       - Logs start/end of each ingestion window (subreddit, window bounds, counts).

4. **State Management (`modules/reddit_ingest/state_store.py`)**
   - `RedditIngestionStateStore` uses AWS SSM Parameter Store to:
     - `load_last_run_timestamp(subreddit)` – read the watermark (or `None` on first run).
     - `save_last_run_timestamp(subreddit, timestamp)` – write/update the watermark.
   - Parameter naming:
     - `/reddit-ingest/{ENVIRONMENT}/{subreddit}/last_run_ts`.
   - Logs errors and invalid state values with sufficient context.

5. **Run Metadata and Metrics (`modules/reddit_ingest/run_metadata.py`)**
   - `RedditIngestionRunRecorder`:
     - Writes a record per window into DynamoDB table `reddit_ingestion_runs`:
       - `pk = "ENV#{environment}#SUBREDDIT#{subreddit}"`.
       - `sk = "RUN#{window_start_ts}"` (or current time if not set).
       - Attributes include `SubmissionsCount`, `CommentsCount`, `Mode` (`incremental`/`backfill`), `WindowStartTs`, `WindowEndTs`, `IngestedAtTs`.
     - Publishes CloudWatch metrics for each window:
       - Namespace: `RedditIngestion`.
       - Metrics:
         - `NewSubmissions` (Count).
         - `NewComments` (Count).
       - Dimensions: `Environment`, `Subreddit`.
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

### `20_lambda`

Located at `aws/us-east-1/20_lambda/`:

- **Backend**: Uses the same S3 backend as other stages with key `20_lambda/terraform.tfstate`.
- **Provider**: Standard AWS provider configured via variables.
- **Data sources**:
  - `terraform_remote_state.s3` – fetch S3 bucket names/ARNs from `10_s3`.
  - `aws_caller_identity.current`, `aws_region.current`.
- **Locals**:
  - `name_prefix` for consistent Lambda-related naming.
  - `landing_bucket_name`, `landing_bucket_arn`, `reddit_prefix = "reddit/"`.
- **Resources**:
  - `aws_lambda_function.reddit_ingestion`:
    - Handler: `handler.lambda_handler`.
    - Runtime: `python3.11`.
    - Deployed from a ZIP artifact built under `build/reddit_ingestion_lambda/`.
    - Environment variables:
      - `LANDING_BUCKET_NAME`, `LANDING_BUCKET_PREFIX`.
      - `REDDIT_SECRET_ARN` (Secrets Manager ARN).
      - `DEFAULT_SUBREDDIT`, `MAX_SUBMISSIONS`.
      - `ENVIRONMENT`, `STATE_BASE_PATH` (SSM path root).
      - `RUNS_TABLE_NAME` (DynamoDB table name).
      - `METRICS_NAMESPACE` (CloudWatch namespace).
      - `MAX_HOURLY_WINDOWS_PER_RUN` (default `"1"`).
  - `aws_cloudwatch_event_rule.reddit_ingestion_schedule`:
    - `schedule_expression` defaults to `rate(1 hour)`.
  - `aws_cloudwatch_event_target.reddit_ingestion_target` and `aws_lambda_permission.allow_events_to_invoke_reddit_ingestion`:
    - Wire EventBridge to the Lambda.
  - `aws_dynamodb_table.reddit_ingestion_runs`:
    - `pk`/`sk` keys, pay-per-request billing, tagged.
  - IAM resources:
    - `aws_iam_role.reddit_ingestion_lambda` with `lambda.amazonaws.com` trust.
    - `aws_iam_policy.reddit_ingestion_inline` attached to the role:
      - CloudWatch Logs (log groups/streams/events).
      - S3 `PutObject` / `AbortMultipartUpload` to the landing bucket under `reddit/`.
      - Secrets Manager `GetSecretValue` for the Reddit credentials secret.
      - SSM `GetParameter` / `PutParameter` under `/reddit-ingest/*`.
      - DynamoDB `PutItem` on the runs table.
      - CloudWatch `PutMetricData` for publishing ingestion metrics.

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
rm -rf build/reddit_ingestion_lambda
mkdir -p build/reddit_ingestion_lambda
cd build/reddit_ingestion_lambda

python3 -m pip install --upgrade pip
python3 -m pip install praw -t .

cp -r ../../modules .
cp ../../lambda/ingestion/handler.py ./handler.py

zip -r reddit_ingestion_lambda.zip .
```

Update `aws/prod.tfvars` to point `reddit_ingestion_lambda_package` to the absolute path of the ZIP if needed.

Then deploy:

```bash
cd ../../aws/us-east-1/10_s3
terraform init
terraform apply -var-file=../../prod.tfvars

cd ../20_lambda
terraform init
terraform apply -var-file=../../prod.tfvars
```

---

## Testing the Ingestion Pipeline

### Manual incremental run

```bash
aws lambda invoke \
  --function-name tf-prod-smartcomp-lambda-reddit-ingestion \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  /tmp/reddit_ingestion_incremental.json

cat /tmp/reddit_ingestion_incremental.json
```

Check:
- SSM: `/reddit-ingest/prod/{subreddit}/last_run_ts`.
- DynamoDB: recent items in `reddit-ingestion-runs` table.
- S3: new JSONL objects under `reddit/{subreddit}/...`.
- CloudWatch Logs: INFO logs for window selection, ingestion, recording, and metrics.

### Backfill over a time range

```bash
START_TS=$(date -d '3 hours ago' +%s)
END_TS=$(date +%s)

aws lambda invoke \
  --function-name tf-prod-smartcomp-lambda-reddit-ingestion \
  --payload "{\"mode\":\"backfill\",\"subreddit\":\"smartphones\",\"start_ts\":${START_TS},\"end_ts\":${END_TS}}" \
  --cli-binary-format raw-in-base64-out \
  /tmp/reddit_ingestion_backfill.json

cat /tmp/reddit_ingestion_backfill.json
```

Expected:
- Multiple hourly windows processed in a single invocation.
- New run items in DynamoDB for each hour.
- SSM watermark unchanged.

If windows are large or many, consider:
- Increasing `MAX_HOURLY_WINDOWS_PER_RUN` (via env var / Terraform).
- Invoking asynchronously (`--invocation-type Event`) and tailing logs.

---

## Logging & Observability

- All core components (`ingestion_service`, `state_store`, `run_metadata`, and the Lambda handler) use Python’s `logging` module:
  - High-level events at `INFO`.
  - Warnings for abnormal-but-non-fatal conditions (e.g., invalid SSM values).
  - Errors for unexpected AWS API or runtime failures.
- Metrics:
  - `NewSubmissions` and `NewComments` per window in CloudWatch under the `RedditIngestion` namespace, with `Environment` and `Subreddit` dimensions.
- Run history:
  - DynamoDB `reddit-ingestion-runs` table allows you to query history per subreddit and environment, order by `sk` for time ordering, and audit ingestion coverage.

This design keeps the ingestion pipeline modular, observable, and aligned with a staged Terraform layout, making it easier to extend later (e.g., sentiment analysis, augmented zone loading) without changing the existing contracts. 


