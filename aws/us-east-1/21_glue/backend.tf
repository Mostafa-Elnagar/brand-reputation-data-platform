terraform {
  backend "s3" {
    bucket = "brandrep-terraform-state-backend"
    key    = "21_glue/terraform.tfstate"
    region = "us-east-1"
  }
}


