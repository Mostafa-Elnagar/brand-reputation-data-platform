locals {
  name_prefix = "${var.prefix}${var.environment}-${var.name}-lambda"

  env = {
    account_id  = data.aws_caller_identity.current.account_id
    region      = data.aws_region.current.name
    environment = var.environment
  }

  default_tags = {
    Region      = data.aws_region.current.name
    Environment = var.environment
    Owner       = "DATA_TEAM"
    Project     = "SMARTCOMP_INGESTION"
    Stage       = "INGESTION"
    ManagedBy   = var.ManagedBy
    CostCenter  = "INGESTION"
  }

  landing_bucket_name = data.terraform_remote_state.s3.outputs.s3_landing_zone_bucket_id
  landing_bucket_arn  = data.terraform_remote_state.s3.outputs.s3_landing_zone_bucket_arn

  reddit_prefix = "reddit/"

  checkpoints_table_name     = data.terraform_remote_state.dynamodb.outputs.reddit_ingestion_checkpoints_table_name
  checkpoints_table_arn      = data.terraform_remote_state.dynamodb.outputs.reddit_ingestion_checkpoints_table_arn
  lambda_run_logs_table_name = data.terraform_remote_state.dynamodb.outputs.lambda_run_logs_table_name
  lambda_run_logs_table_arn  = data.terraform_remote_state.dynamodb.outputs.lambda_run_logs_table_arn
}
