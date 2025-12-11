# =============================================================================
# Reference Data Upload
# =============================================================================

# Upload the CSV directly to S3 as a seed file
# The Glue job/Iceberg table expects Parquet/Iceberg format usually, but we can have an initial job convert it 
# OR we can upload it as a raw file and have the job read it as CSV if needed.
# For simplicity, let's upload the CSV to a 'seed' location.

resource "aws_s3_object" "ref_data_csv" {
  bucket = local.lakehouse_bucket_name
  key    = "silver/ref_data/seeds/smartphones_models.csv"
  source = "${path.module}/../../../ref_data/smartphones_models.csv"
  etag   = filemd5("${path.module}/../../../ref_data/smartphones_models.csv")

  tags = local.default_tags
}

