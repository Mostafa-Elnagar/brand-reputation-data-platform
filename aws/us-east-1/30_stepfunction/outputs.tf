output "reddit_listing_ingestion_state_machine_arn" {
  description = "ARN of the Reddit listing ingestion Step Functions state machine."
  value       = aws_sfn_state_machine.reddit_listing_ingestion.arn
}

output "state_machine_arn" {
  description = "ARN of the Reddit listing ingestion Step Functions state machine (alias)."
  value       = aws_sfn_state_machine.reddit_listing_ingestion.arn
}
