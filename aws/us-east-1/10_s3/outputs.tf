# Landing Zone Bucket
output "s3_landing_zone_bucket_id" {
  description = "The name of the bucket"
  value       = module.landing_zone_bucket.s3_bucket_id
}
output "s3_landing_zone_bucket_arn" {
  description = "The ARN of the bucket formatted as (arn:aws:s3:::bucket-name)"
  value       = module.landing_zone_bucket.s3_bucket_arn
}

# Archived Zone Bucket
output "s3_augmented_zone_bucket_id" {
  description = "The name of the bucket"
  value       = module.augmented_zone_bucket.s3_bucket_id
}
output "s3_augmented_zone_bucket_arn" {
  description = "The ARN of the bucket formatted as (arn:aws:s3:::bucket-name)"
  value       = module.augmented_zone_bucket.s3_bucket_arn
}

# Curated Zone Bucket
output "s3_curated_zone_bucket_id" {
  description = "The name of the bucket"
  value       = module.curated_zone_bucket.s3_bucket_id
}
output "s3_curated_zone_bucket_arn" {
  description = "The ARN of the bucket formatted as (arn:aws:s3:::bucket-name)"
  value       = module.curated_zone_bucket.s3_bucket_arn
}