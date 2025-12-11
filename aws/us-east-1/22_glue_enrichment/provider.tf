provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "SMARTCOMP_DATA_LAKE"
      Environment = var.environment
      ManagedBy   = var.ManagedBy
    }
  }
}
