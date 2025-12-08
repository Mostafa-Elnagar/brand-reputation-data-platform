data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "terraform_remote_state" "s3" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "10_s3/terraform.tfstate"
    region = "us-east-1"
  }
}


