resource "aws_cloudwatch_event_rule" "reddit_ingestion_schedule" {
  name                = "${var.prefix}${var.name}-reddit-ingestion-schedule"
  description         = "Schedule to trigger Reddit ingestion Lambda."
  schedule_expression = var.reddit_ingestion_schedule_expression

  tags = local.default_tags
}

resource "aws_cloudwatch_event_target" "reddit_ingestion_target" {
  rule      = aws_cloudwatch_event_rule.reddit_ingestion_schedule.name
  target_id = "reddit-ingestion-lambda"
  arn       = aws_lambda_function.reddit_ingestion.arn
}

resource "aws_lambda_permission" "allow_events_to_invoke_reddit_ingestion" {
  statement_id  = "AllowExecutionFromCloudWatchEvents"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reddit_ingestion.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reddit_ingestion_schedule.arn
}


