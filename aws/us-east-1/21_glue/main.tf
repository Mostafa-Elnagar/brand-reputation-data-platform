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

# =============================================================================
# Bronze Submissions Table (Iceberg)
# =============================================================================

resource "aws_glue_catalog_table" "submissions_bronze" {
  name          = "submissions"
  database_name = aws_glue_catalog_database.bronze.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"            = "parquet"
    "write_compression" = "zstd"
    "classification"    = "parquet"
  }

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = local.bronze_submissions_path

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "fullname_id"
      type = "string"
    }
    columns {
      name = "subreddit"
      type = "string"
    }
    columns {
      name = "author"
      type = "string"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "selftext"
      type = "string"
    }
    columns {
      name = "created_utc"
      type = "bigint"
    }
    columns {
      name = "updated_at"
      type = "bigint"
    }
    columns {
      name = "score"
      type = "int"
    }
    columns {
      name = "upvote_ratio"
      type = "double"
    }
    columns {
      name = "num_comments"
      type = "int"
    }
    columns {
      name = "is_self"
      type = "boolean"
    }
    columns {
      name = "stickied"
      type = "boolean"
    }
    columns {
      name = "locked"
      type = "boolean"
    }
    columns {
      name = "distinguished"
      type = "string"
    }
    columns {
      name = "permalink"
      type = "string"
    }
    columns {
      name = "url"
      type = "string"
    }
    columns {
      name = "edited"
      type = "bigint"
    }
    columns {
      name = "is_video"
      type = "boolean"
    }
    columns {
      name = "media_only"
      type = "boolean"
    }
    columns {
      name = "thumbnail"
      type = "string"
    }
    columns {
      name = "ingestion_timestamp"
      type = "timestamp"
    }
    columns {
      name = "source_sort_type"
      type = "string"
    }
    columns {
      name = "source_file"
      type = "string"
    }
    columns {
      name = "partition_date"
      type = "string"
    }
  }
}

# =============================================================================
# Bronze Comments Table (Iceberg)
# =============================================================================

resource "aws_glue_catalog_table" "comments_bronze" {
  name          = "comments"
  database_name = aws_glue_catalog_database.bronze.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "format"            = "parquet"
    "write_compression" = "zstd"
    "classification"    = "parquet"
  }

  open_table_format_input {
    iceberg_input {
      metadata_operation = "CREATE"
      version            = "2"
    }
  }

  storage_descriptor {
    location = local.bronze_comments_path

    columns {
      name = "id"
      type = "string"
    }
    columns {
      name = "fullname_id"
      type = "string"
    }
    columns {
      name = "submission_id"
      type = "string"
    }
    columns {
      name = "link_id"
      type = "string"
    }
    columns {
      name = "parent_id"
      type = "string"
    }
    columns {
      name = "author"
      type = "string"
    }
    columns {
      name = "body"
      type = "string"
    }
    columns {
      name = "created_utc"
      type = "bigint"
    }
    columns {
      name = "updated_at"
      type = "bigint"
    }
    columns {
      name = "score"
      type = "int"
    }
    columns {
      name = "controversiality"
      type = "int"
    }
    columns {
      name = "is_submitter"
      type = "boolean"
    }
    columns {
      name = "distinguished"
      type = "string"
    }
    columns {
      name = "edited"
      type = "bigint"
    }
    columns {
      name = "permalink"
      type = "string"
    }
    columns {
      name = "depth"
      type = "int"
    }
    columns {
      name = "ingestion_timestamp"
      type = "timestamp"
    }
    columns {
      name = "source_sort_type"
      type = "string"
    }
    columns {
      name = "source_file"
      type = "string"
    }
    columns {
      name = "partition_date"
      type = "string"
    }
  }
}

# =============================================================================
# Glue Script Upload to S3
# =============================================================================

resource "aws_s3_object" "bronze_ingestion_script" {
  bucket = local.augmented_bucket_name
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
    script_location = "s3://${local.augmented_bucket_name}/${aws_s3_object.bronze_ingestion_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${local.augmented_bucket_name}/glue/spark-logs/"
    "--TempDir"                          = "s3://${local.augmented_bucket_name}/glue/temp/"
    "--LANDING_BUCKET"                   = local.landing_bucket_name
    "--DATABASE_NAME"                    = aws_glue_catalog_database.bronze.name
    "--datalake-formats"                 = "iceberg"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.default_tags
}
