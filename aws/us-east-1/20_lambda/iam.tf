data "aws_iam_policy_document" "reddit_ingestion_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "reddit_ingestion_lambda" {
  name               = "${var.prefix}reddit-ingestion-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.reddit_ingestion_assume_role.json

  tags = local.default_tags
}

data "aws_iam_policy_document" "reddit_ingestion_policy" {
  statement {
    sid = "AllowCloudWatchLogs"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    sid = "AllowPutToLandingBucket"

    actions = [
      "s3:PutObject",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      "${local.landing_bucket_arn}/${local.reddit_prefix}*",
    ]
  }

  statement {
    sid = "AllowReadRedditSecret"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [var.reddit_secret_arn]
  }

  statement {
    sid = "AllowWriteIngestionRuns"

    actions = [
      "dynamodb:PutItem",
    ]

    resources = [
      local.lambda_run_logs_table_arn,
    ]
  }

  statement {
    sid = "AllowReadWriteIngestionCheckpoints"

    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
    ]

    resources = [
      local.checkpoints_table_arn,
    ]
  }

  statement {
    sid = "AllowPublishIngestionMetrics"

    actions = [
      "cloudwatch:PutMetricData",
    ]

    resources = [
      "*",
    ]
  }
}

resource "aws_iam_policy" "reddit_ingestion_inline" {
  name   = "${var.prefix}reddit-ingestion-lambda-policy"
  policy = data.aws_iam_policy_document.reddit_ingestion_policy.json
}

resource "aws_iam_role_policy_attachment" "reddit_ingestion_attach" {
  role       = aws_iam_role.reddit_ingestion_lambda.name
  policy_arn = aws_iam_policy.reddit_ingestion_inline.arn
}
