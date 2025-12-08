locals {
  name_prefix = "${var.prefix}${var.environment}-${var.name}-sfn"

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
    Stage       = "LISTING_INGESTION"
    ManagedBy   = var.ManagedBy
    CostCenter  = "LISTING_INGESTION"
  }
}
