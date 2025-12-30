output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule for Reddit ingestion scheduling."
  value       = aws_cloudwatch_event_rule.reddit_ingestion_schedule.arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule for Reddit ingestion scheduling."
  value       = aws_cloudwatch_event_rule.reddit_ingestion_schedule.name
}

output "eventbridge_stepfunction_role_arn" {
  description = "ARN of the IAM role used by EventBridge to start Step Function."
  value       = aws_iam_role.eventbridge_stepfunction_role.arn
}



