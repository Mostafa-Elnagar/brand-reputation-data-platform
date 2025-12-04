module "landing_zone_bucket" {
  source              = "terraform-aws-modules/s3-bucket/aws"
  acceleration_status = "Suspended"
  bucket              = "${local.name_prefix}-lz"

  # S3 bucket-level Public Access Block configuration
  block_public_acls                    = true
  block_public_policy                  = true
  ignore_public_acls                   = true
  restrict_public_buckets              = true
  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm     = "AES256"
      }
    }
  }
  expected_bucket_owner = data.aws_caller_identity.current.account_id
  tags                  = local.default_tags
}

module "augmented_zone_bucket" {
  source              = "terraform-aws-modules/s3-bucket/aws"
  acceleration_status = "Suspended"
  bucket              = "${local.name_prefix}-augmented"

  # S3 bucket-level Public Access Block configuration
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm     = "AES256"
      }
    }
  }
  expected_bucket_owner = data.aws_caller_identity.current.account_id
  tags                  = local.default_tags
}

module "curated_zone_bucket" {
  source              = "terraform-aws-modules/s3-bucket/aws"
  acceleration_status = "Suspended"
  bucket              = "${local.name_prefix}-curated"

  # S3 bucket-level Public Access Block configuration
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

  server_side_encryption_configuration = {
    rule = {
      apply_server_side_encryption_by_default = {
        sse_algorithm     = "AES256"
      }
    }
  }
  expected_bucket_owner = data.aws_caller_identity.current.account_id
  tags                  = local.default_tags
}