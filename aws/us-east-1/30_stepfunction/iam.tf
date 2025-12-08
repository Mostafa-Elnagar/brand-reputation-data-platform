data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.${data.aws_region.current.name}.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "reddit_listing_ingestion_role" {
  name               = "${var.prefix}reddit-listing-ingestion-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json

  tags = local.default_tags
}

data "aws_iam_policy_document" "sfn_policy" {
  statement {
    sid = "AllowInvokeRedditIngestionLambda"

    actions = [
      "lambda:InvokeFunction",
    ]

    resources = [
      data.terraform_remote_state.lambda.outputs.reddit_ingestion_lambda_arn,
    ]
  }
}

resource "aws_iam_policy" "reddit_listing_ingestion_policy" {
  name   = "${var.prefix}reddit-listing-ingestion-sfn-policy"
  policy = data.aws_iam_policy_document.sfn_policy.json
}

resource "aws_iam_role_policy_attachment" "reddit_listing_ingestion_attach" {
  role       = aws_iam_role.reddit_listing_ingestion_role.name
  policy_arn = aws_iam_policy.reddit_listing_ingestion_policy.arn
}
