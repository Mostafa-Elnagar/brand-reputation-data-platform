terraform {
  backend "s3" {
    bucket         = "brandrep-terraform-state-backend"
    key            = "22_glue_enrichment/terraform.tfstate"
    region         = "us-east-1"
  }
}
