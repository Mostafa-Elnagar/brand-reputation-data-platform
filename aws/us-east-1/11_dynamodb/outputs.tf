output "reddit_ingestion_checkpoints_table_name" {
  description = "DynamoDB table name for Reddit ingestion checkpoints."
  value       = aws_dynamodb_table.reddit_ingestion_checkpoints.name
}

output "reddit_ingestion_checkpoints_table_arn" {
  description = "DynamoDB table ARN for Reddit ingestion checkpoints."
  value       = aws_dynamodb_table.reddit_ingestion_checkpoints.arn
}

output "lambda_run_logs_table_name" {
  description = "DynamoDB table name for generic Lambda run logs."
  value       = aws_dynamodb_table.lambda_run_logs.name
}

output "lambda_run_logs_table_arn" {
  description = "DynamoDB table ARN for generic Lambda run logs."
  value       = aws_dynamodb_table.lambda_run_logs.arn
}
