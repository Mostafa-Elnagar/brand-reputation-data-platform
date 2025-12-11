locals {
  name_prefix = "${var.prefix}${var.environment}-${var.name}-glue-enrichment"

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
    Stage       = "SILVER"
    ManagedBy   = var.ManagedBy
    CostCenter  = "DATA_LAKE"
  }

  lakehouse_bucket_name = data.terraform_remote_state.s3.outputs.s3_lakehouse_bucket_id
  lakehouse_bucket_arn  = data.terraform_remote_state.s3.outputs.s3_lakehouse_bucket_arn

  # Bronze Paths
  bronze_submissions_path = "s3://${local.lakehouse_bucket_name}/bronze/reddit_submissions/"
  bronze_comments_path    = "s3://${local.lakehouse_bucket_name}/bronze/reddit_comments/"

  # Silver Paths
  silver_sentiment_path   = "s3://${local.lakehouse_bucket_name}/silver/sentiment_analysis/"
  silver_ref_data_path    = "s3://${local.lakehouse_bucket_name}/silver/ref_data/"
  silver_submissions_path = "s3://${local.lakehouse_bucket_name}/silver/submissions/"
  silver_comments_path    = "s3://${local.lakehouse_bucket_name}/silver/comments/"

  glue_scripts_path = "s3://${local.lakehouse_bucket_name}/glue/scripts/"

  bronze_database_name = data.terraform_remote_state.glue.outputs.bronze_database_name
}
