terraform {
  backend "s3" {
    bucket = "brandrep-terraform-state-backend"
    key    = "30_stepfunction/terraform.tfstate"
    region = "us-east-1"
  }
}


