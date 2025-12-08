output "reddit_ingestion_lambda_name" {
  description = "Name of the Reddit ingestion Lambda function."
  value       = aws_lambda_function.reddit_ingestion.function_name
}

output "reddit_ingestion_lambda_arn" {
  description = "ARN of the Reddit ingestion Lambda function."
  value       = aws_lambda_function.reddit_ingestion.arn
}


