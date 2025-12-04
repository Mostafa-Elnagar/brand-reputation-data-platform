terraform {
  backend "s3" {
    bucket = "dashdash-terraform-state-backend"
    key    = "20_lambda/terraform.tfstate"
    region = "us-east-1"
  }
}


