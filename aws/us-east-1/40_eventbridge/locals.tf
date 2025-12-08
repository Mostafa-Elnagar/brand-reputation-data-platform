locals {
  name_prefix = "${var.prefix}${var.environment}-${var.name}-eb"

  default_tags = {
    Region      = data.aws_region.current.name
    Environment = var.environment
    Owner       = "DATA_TEAM"
    Project     = "SMARTCOMP_DATA_LAKE"
    Stage       = "EVENTBRIDGE"
    ManagedBy   = var.ManagedBy
    CostCenter  = "DATALAKE"
  }
}


