# Landing Zone Bucket
output "s3_landing_zone_bucket_id" {
  description = "The name of the bucket"
  value       = module.landing_zone_bucket.s3_bucket_id
}
output "s3_landing_zone_bucket_arn" {
  description = "The ARN of the bucket formatted as (arn:aws:s3:::bucket-name)"
  value       = module.landing_zone_bucket.s3_bucket_arn
}

# Lakehouse Bucket
output "s3_lakehouse_bucket_id" {
  description = "The name of the lakehouse bucket"
  value       = module.lakehouse_bucket.s3_bucket_id
}
output "s3_lakehouse_bucket_arn" {
  description = "The ARN of the lakehouse bucket formatted as (arn:aws:s3:::bucket-name)"
  value       = module.lakehouse_bucket.s3_bucket_arn
}
