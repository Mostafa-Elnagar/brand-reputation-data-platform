data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "terraform_remote_state" "stepfunction" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "30_stepfunction/terraform.tfstate"
    region = "us-east-1"
  }
}



