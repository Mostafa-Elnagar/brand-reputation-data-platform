terraform {
    backend "s3" {
        bucket = "brandrep-terraform-state-backend"
        key = "40_eventbridge/terraform.tfstate"
        region = "us-east-1"
    }
}


