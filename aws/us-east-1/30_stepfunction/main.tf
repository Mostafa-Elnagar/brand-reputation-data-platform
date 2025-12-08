locals {
  # Generate all (subreddit, sort_type) combinations
  invocations = flatten([
    for subreddit in var.subreddits : [
      for sort_type in var.sort_types : {
        subreddit = subreddit
        sort_type = sort_type
      }
    ]
  ])
}

resource "aws_sfn_state_machine" "reddit_listing_ingestion" {
  name     = "${local.name_prefix}-reddit-listing-ingestion"
  role_arn = aws_iam_role.reddit_listing_ingestion_role.arn

  definition = templatefile("${path.module}/listing_ingestion_state_machine.asl.json", {
    lambda_function_arn = data.terraform_remote_state.lambda.outputs.reddit_ingestion_lambda_arn
    invocations_json    = jsonencode(local.invocations)
  })

  tags = local.default_tags
}
