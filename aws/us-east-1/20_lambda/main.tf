resource "aws_lambda_function" "reddit_ingestion" {
  function_name = "${local.name_prefix}-reddit-ingestion"
  role          = aws_iam_role.reddit_ingestion_lambda.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"

  filename         = var.reddit_ingestion_lambda_package
  source_code_hash = filebase64sha256(var.reddit_ingestion_lambda_package)

  timeout      = 900
  memory_size  = 512
  description  = "Fetch Reddit listings (top/controversial) and write JSONL files to S3."

  environment {
    variables = {
      LANDING_BUCKET_NAME    = local.landing_bucket_name
      LANDING_BUCKET_PREFIX  = local.reddit_prefix
      REDDIT_SECRET_ARN      = var.reddit_secret_arn
      DEFAULT_SUBREDDIT      = var.default_subreddit
      MAX_SUBMISSIONS        = tostring(var.max_submissions)
      ENVIRONMENT            = var.environment
      RUNS_TABLE_NAME        = local.lambda_run_logs_table_name
      CHECKPOINTS_TABLE_NAME = local.checkpoints_table_name
      METRICS_NAMESPACE      = "RedditIngestion"
    }
  }

  tags = local.default_tags
}
