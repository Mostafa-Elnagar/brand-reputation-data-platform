# =============================================================================
# Landing Zone Database and Crawlers
# =============================================================================

resource "aws_glue_catalog_database" "reddit_landing" {
  name = "${local.name_prefix}-reddit-landing-db"

  tags = local.default_tags
}

resource "aws_glue_crawler" "reddit_submissions" {
  name          = "${local.name_prefix}-reddit-submissions-crawler"
  database_name = aws_glue_catalog_database.reddit_landing.name
  role          = aws_iam_role.glue_crawler_role.arn
  table_prefix  = "submissions_"

  s3_target {
    path = "s3://${local.landing_bucket_name}/reddit/"

    exclusions = [
      "**/*comments.jsonl",
    ]
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = local.default_tags
}

resource "aws_glue_crawler" "reddit_comments" {
  name          = "${local.name_prefix}-reddit-comments-crawler"
  database_name = aws_glue_catalog_database.reddit_landing.name
  role          = aws_iam_role.glue_crawler_role.arn
  table_prefix  = "comments_"

  s3_target {
    path = "s3://${local.landing_bucket_name}/reddit/"

    exclusions = [
      "**/*submissions.jsonl",
    ]
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = local.default_tags
}

# =============================================================================
# Bronze Layer Database (Iceberg)
# =============================================================================

resource "aws_glue_catalog_database" "bronze" {
  name = "brandrep_bronze"

  tags = local.default_tags
}

# Note: Iceberg tables (submissions, comments) are created by the Glue job
# to ensure correct metadata initialization. Defining them here causes
# issues with missing metadata files.

# =============================================================================
# Glue Script Upload to S3
# =============================================================================

resource "aws_s3_object" "bronze_ingestion_script" {
  bucket = local.lakehouse_bucket_name
  key    = "glue/scripts/reddit_bronze_ingestion.py"
  source = "${path.module}/scripts/reddit_bronze_ingestion.py"
  etag   = filemd5("${path.module}/scripts/reddit_bronze_ingestion.py")
}

# =============================================================================
# Bronze Ingestion Glue Job
# =============================================================================

resource "aws_glue_job" "reddit_bronze_ingestion" {
  name     = "${local.name_prefix}-reddit-bronze-ingestion"
  role_arn = aws_iam_role.glue_job_role.arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${local.lakehouse_bucket_name}/${aws_s3_object.bronze_ingestion_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.lakehouse_bucket_name}/glue/spark-logs/"
    "--TempDir"                          = "s3://${local.lakehouse_bucket_name}/glue/temp/"
    "--LANDING_BUCKET"                   = local.landing_bucket_name
    "--DATABASE_NAME"                    = aws_glue_catalog_database.bronze.name
    "--AUGMENTED_BUCKET"                 = local.lakehouse_bucket_name
    "--METRICS_TABLE_NAME"               = local.glue_metrics_table_name
    "--ENVIRONMENT"                      = var.environment
    "--METRICS_NAMESPACE"                = "RedditBronzeIngestion"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.default_tags
}
