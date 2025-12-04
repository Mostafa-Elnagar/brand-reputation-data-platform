resource "aws_lambda_function" "reddit_ingestion" {
  function_name = "${local.name_prefix}-reddit-ingestion"
  role          = aws_iam_role.reddit_ingestion_lambda.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"

  filename         = var.reddit_ingestion_lambda_package
  source_code_hash = filebase64sha256(var.reddit_ingestion_lambda_package)

  timeout      = 300
  memory_size  = 512
  description  = "Fetch Reddit data and write JSONL files to the landing zone bucket."

  environment {
    variables = {
      LANDING_BUCKET_NAME   = local.landing_bucket_name
      LANDING_BUCKET_PREFIX = local.reddit_prefix
      REDDIT_SECRET_ARN     = var.reddit_secret_arn
      DEFAULT_SUBREDDIT     = var.default_subreddit
      MAX_SUBMISSIONS       = tostring(var.max_submissions)

      ENVIRONMENT     = var.environment
      STATE_BASE_PATH = "/reddit-ingest"
      RUNS_TABLE_NAME = aws_dynamodb_table.reddit_ingestion_runs.name
      METRICS_NAMESPACE = "RedditIngestion"
      MAX_HOURLY_WINDOWS_PER_RUN = "1"
    }
  }

  tags = local.default_tags
}


