data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "terraform_remote_state" "lambda" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "20_lambda/terraform.tfstate"
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

data "terraform_remote_state" "glue_enrichment" {
  backend = "s3"
  config = {
    bucket = "brandrep-terraform-state-backend"
    key    = "22_glue_enrichment/terraform.tfstate"
    region = var.region
  }
}
