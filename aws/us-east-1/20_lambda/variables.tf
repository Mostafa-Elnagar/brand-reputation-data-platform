variable "prefix" {}
variable "name" {}
variable "owner" {}
variable "environment" {}
variable "region" {}
variable "vpc_cidr" {}
variable "ManagedBy" {}

variable "reddit_ingestion_lambda_package" {
  description = "Path to the zipped Reddit ingestion Lambda package."
  type        = string
}

variable "reddit_secret_arn" {
  description = "ARN of the secret containing Reddit API credentials."
  type        = string
}

variable "default_subreddit" {
  description = "Default subreddit to ingest when not provided in the event."
  type        = string
  default     = "smartphones"
}

variable "max_submissions" {
  description = "Maximum number of submissions to ingest per run."
  type        = number
  default     = 1000
}
