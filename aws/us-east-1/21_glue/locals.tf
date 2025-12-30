locals {
  name_prefix = "${var.prefix}${var.environment}-${var.name}-glue"

  env = {
    account_id  = data.aws_caller_identity.current.account_id
    region      = data.aws_region.current.name
    environment = var.environment
  }

  default_tags = {
    Region      = data.aws_region.current.name
    Environment = var.environment
    Owner       = "DATA_TEAM"
    Project     = "SMARTCOMP_DATA_LAKE"
    Stage       = "BRONZE"
    ManagedBy   = var.ManagedBy
    CostCenter  = "DATA_LAKE"
  }

  landing_bucket_name   = data.terraform_remote_state.s3.outputs.s3_landing_zone_bucket_id
  landing_bucket_arn    = data.terraform_remote_state.s3.outputs.s3_landing_zone_bucket_arn
  lakehouse_bucket_name = data.terraform_remote_state.s3.outputs.s3_lakehouse_bucket_id
  lakehouse_bucket_arn  = data.terraform_remote_state.s3.outputs.s3_lakehouse_bucket_arn

  bronze_submissions_path = "s3://${local.lakehouse_bucket_name}/bronze/reddit_submissions/"
  bronze_comments_path    = "s3://${local.lakehouse_bucket_name}/bronze/reddit_comments/"
  glue_scripts_path       = "s3://${local.lakehouse_bucket_name}/glue/scripts/"

  # DynamoDB tables for metrics
  glue_metrics_table_name = data.terraform_remote_state.dynamodb.outputs.glue_ingestion_metrics_table_name
  glue_metrics_table_arn  = data.terraform_remote_state.dynamodb.outputs.glue_ingestion_metrics_table_arn
}


