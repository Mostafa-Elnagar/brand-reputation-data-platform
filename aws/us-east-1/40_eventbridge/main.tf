resource "aws_cloudwatch_event_rule" "reddit_ingestion_schedule" {
  name                = "${local.name_prefix}-reddit-ingest"
  description         = "Schedule to trigger Reddit listing ingestion Step Function every 3 hours."
  schedule_expression = var.reddit_ingestion_schedule_expression

  tags = local.default_tags
}

resource "aws_cloudwatch_event_target" "reddit_ingestion_stepfunction" {
  rule      = aws_cloudwatch_event_rule.reddit_ingestion_schedule.name
  target_id = "reddit-sfn"
  arn       = data.terraform_remote_state.stepfunction.outputs.state_machine_arn
  role_arn  = aws_iam_role.eventbridge_stepfunction_role.arn

  input = jsonencode({})
}
