terraform {
  backend "s3" {
    bucket = "brandrep-terraform-state-backend"
    key    = "11_dynamodb/terraform.tfstate"
    region = "us-east-1"
  }
}


