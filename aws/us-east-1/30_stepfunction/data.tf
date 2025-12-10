data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "terraform_remote_state" "lambda" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "20_lambda/terraform.tfstate"
    region = "us-east-1"
  }
}

data "terraform_remote_state" "glue" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "21_glue/terraform.tfstate"
    region = "us-east-1"
  }
}
