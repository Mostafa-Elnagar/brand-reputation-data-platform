# =============================================================================
# IAM Role for Silver Glue Job
# =============================================================================

resource "aws_iam_role" "glue_silver_role" {
  name = "${local.name_prefix}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = local.default_tags
}

# =============================================================================
# IAM Policies
# =============================================================================

resource "aws_iam_policy" "glue_silver_s3_policy" {
  name        = "${local.name_prefix}-s3-policy"
  description = "Policy for Glue Silver job to access S3 buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          local.lakehouse_bucket_arn,
          "${local.lakehouse_bucket_arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "glue_silver_secrets_policy" {
  name        = "${local.name_prefix}-secrets-policy"
  description = "Policy for Glue Silver job to access Secrets Manager"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          var.gemini_api_key_secret_arn
        ]
      }
    ]
  })
}

resource "aws_iam_policy" "glue_silver_cloudwatch_policy" {
  name        = "${local.name_prefix}-cloudwatch-policy"
  description = "Policy for Glue Silver job logging and metrics"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*:/aws-glue/*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = [
              "RedditSilverCore",
              "RedditSilverSentiment"
            ]
          }
        }
      }
    ]
  })
}

# =============================================================================
# Policy Attachments
# =============================================================================

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_silver_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "secrets_access" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_silver_secrets_policy.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch_access" {
  role       = aws_iam_role.glue_silver_role.name
  policy_arn = aws_iam_policy.glue_silver_cloudwatch_policy.arn
}
