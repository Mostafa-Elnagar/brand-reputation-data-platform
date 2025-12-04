locals {
  name_prefix = "${var.prefix}${var.environment}-${var.name}-${var.region}-s3"

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
    Stage       = "DATA_LAKE"
    ManagedBy   = var.ManagedBy
    CostCenter  = "DATALAKE"
  }
  azs = slice(data.aws_availability_zones.available.names, 0, 3)
}