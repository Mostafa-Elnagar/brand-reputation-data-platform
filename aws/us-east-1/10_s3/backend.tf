terraform {
    backend "s3" {
        bucket = "brandrep-terraform-state-backend"
        key = "10_s3/terraform.tfstate"
        region = "us-east-1"
    }
}