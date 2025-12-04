resource "aws_dynamodb_table" "reddit_ingestion_runs" {
  name         = "${var.prefix}${var.name}-reddit-ingestion-runs"
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


