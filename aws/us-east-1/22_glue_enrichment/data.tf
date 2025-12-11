data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "terraform_remote_state" "s3" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "10_s3/terraform.tfstate"
    region = var.region
  }
}

data "terraform_remote_state" "dynamodb" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "11_dynamodb/terraform.tfstate"
    region = var.region
  }
}

data "terraform_remote_state" "glue" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "21_glue/terraform.tfstate"
    region = var.region
  }
}
