# =============================================================================
# Landing Zone Outputs
# =============================================================================

output "reddit_landing_database_name" {
  description = "Glue database name for Reddit landing data."
  value       = aws_glue_catalog_database.reddit_landing.name
}

output "reddit_submissions_crawler_name" {
  description = "Glue crawler name for Reddit submissions JSONL."
  value       = aws_glue_crawler.reddit_submissions.name
}

output "reddit_comments_crawler_name" {
  description = "Glue crawler name for Reddit comments JSONL."
  value       = aws_glue_crawler.reddit_comments.name
}

# =============================================================================
# Bronze Layer Outputs
# =============================================================================

output "bronze_database_name" {
  description = "Glue database name for Bronze layer Iceberg tables."
  value       = aws_glue_catalog_database.bronze.name
}

output "submissions_bronze_table_name" {
  description = "Bronze Iceberg table name for Reddit submissions."
  value       = "submissions"
}

output "comments_bronze_table_name" {
  description = "Bronze Iceberg table name for Reddit comments."
  value       = "comments"
}

output "bronze_ingestion_job_name" {
  description = "Glue job name for Bronze layer ingestion."
  value       = aws_glue_job.reddit_bronze_ingestion.name
}

output "bronze_ingestion_job_arn" {
  description = "ARN of the Glue job for Bronze layer ingestion."
  value       = aws_glue_job.reddit_bronze_ingestion.arn
}
