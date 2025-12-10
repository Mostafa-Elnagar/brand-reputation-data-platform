resource "aws_dynamodb_table" "reddit_ingestion_checkpoints" {
  name         = "${local.name_prefix}-reddit-ingestion-checkpoints"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  tags = local.default_tags
}

resource "aws_dynamodb_table" "lambda_run_logs" {
  name         = "${local.name_prefix}-lambda-run-logs"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  tags = local.default_tags
}

resource "aws_dynamodb_table" "glue_ingestion_metrics" {
  name         = "${local.name_prefix}-glue-ingestion-metrics"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "pk"
  range_key = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  tags = local.default_tags
}
