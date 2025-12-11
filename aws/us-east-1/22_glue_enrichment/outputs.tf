output "silver_database_name" {
  description = "Name of the Silver Glue database"
  value       = aws_glue_catalog_database.silver.name
}

output "sentiment_analysis_table_name" {
  description = "Name of the sentiment analysis Iceberg table"
  value       = aws_glue_catalog_table.sentiment_analysis.name
}

output "silver_core_job_name" {
  description = "Name of the Silver core cleaning Glue job"
  value       = aws_glue_job.reddit_silver_core.name
}

output "silver_core_job_arn" {
  description = "ARN of the Silver core cleaning Glue job"
  value       = aws_glue_job.reddit_silver_core.arn
}

output "silver_sentiment_job_name" {
  description = "Name of the Silver sentiment analysis Glue job"
  value       = aws_glue_job.reddit_silver_sentiment.name
}

output "silver_sentiment_job_arn" {
  description = "ARN of the Silver sentiment analysis Glue job"
  value       = aws_glue_job.reddit_silver_sentiment.arn
}
