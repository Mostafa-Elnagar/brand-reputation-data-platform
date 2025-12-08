variable "prefix" {}
variable "name" {}
variable "owner" {}
variable "environment" {}
variable "region" {}
variable "vpc_cidr" {}
variable "ManagedBy" {}

variable "reddit_ingestion_schedule_expression" {
  description = "EventBridge schedule expression for Reddit ingestion (daily at midnight UTC)."
  type        = string
  default     = "cron(0 0 * * ? *)"
}
