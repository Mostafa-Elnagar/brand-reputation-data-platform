# =============================================================================
# Glue Crawler IAM Role (for Landing Zone crawlers)
# =============================================================================

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler_role" {
  name               = "${var.prefix}reddit-glue-crawler-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = local.default_tags
}

data "aws_iam_policy_document" "glue_crawler_policy" {
  statement {
    sid = "AllowReadLandingBucket"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      local.landing_bucket_arn,
      "${local.landing_bucket_arn}/reddit/*",
    ]
  }

  statement {
    sid = "AllowGlueCatalogAccess"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:BatchGetPartition",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
    ]

    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
    ]
  }

  statement {
    sid = "AllowGlueCrawlerLogs"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/crawlers*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/crawlers:log-stream:*",
    ]
  }
}

resource "aws_iam_policy" "glue_crawler_policy" {
  name   = "${var.prefix}reddit-glue-crawler-policy"
  policy = data.aws_iam_policy_document.glue_crawler_policy.json
}

resource "aws_iam_role_policy_attachment" "glue_crawler_attach" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_crawler_policy.arn
}

# =============================================================================
# Glue Job IAM Role (for Bronze Ingestion)
# =============================================================================

resource "aws_iam_role" "glue_job_role" {
  name               = "${var.prefix}reddit-glue-job-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  tags = local.default_tags
}

data "aws_iam_policy_document" "glue_job_policy" {
  statement {
    sid = "AllowReadLandingBucket"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [
      local.landing_bucket_arn,
      "${local.landing_bucket_arn}/*",
    ]
  }

  statement {
    sid = "AllowReadWriteAugmentedBucket"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]

    resources = [
      local.augmented_bucket_arn,
      "${local.augmented_bucket_arn}/*",
    ]
  }

  statement {
    sid = "AllowGlueCatalogFullAccess"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:CreateDatabase",
      "glue:UpdateDatabase",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetTable",
      "glue:GetTables",
      "glue:BatchGetPartition",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
    ]

    resources = [
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
      "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*",
    ]
  }

  statement {
    sid = "AllowGlueJobLogs"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
      "logs:GetLogEvents",
    ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }

  statement {
    sid = "AllowCloudWatchMetrics"

    actions = [
      "cloudwatch:PutMetricData",
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_job_policy" {
  name   = "${var.prefix}reddit-glue-job-policy"
  policy = data.aws_iam_policy_document.glue_job_policy.json
}

resource "aws_iam_role_policy_attachment" "glue_job_attach" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_job_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_job_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
