# =============================================================================
# Silver Layer Database
# =============================================================================

resource "aws_glue_catalog_database" "silver" {
  name = "brandrep_silver"
  tags = local.default_tags
}

# =============================================================================
# Silver-core Tables (Cleaned Bronze data)
# =============================================================================

resource "aws_glue_catalog_table" "submissions_silver" {
  name          = "submissions_silver"
  database_name = aws_glue_catalog_database.silver.name

  table_type = "ICEBERG"

  parameters = {
    "format"      = "parquet"
    "table_type"  = "ICEBERG"
  }

  storage_descriptor {
    location = local.silver_submissions_path

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
      name = "is_usable_for_sentiment"
      type = "boolean"
    }
    columns {
      name = "partition_date"
      type = "string"
    }
  }

  partition_keys {
    name = "partition_date"
    type = "string"
  }
}

resource "aws_glue_catalog_table" "comments_silver" {
  name          = "comments_silver"
  database_name = aws_glue_catalog_database.silver.name

  table_type = "ICEBERG"

  parameters = {
    "format"      = "parquet"
    "table_type"  = "ICEBERG"
  }

  storage_descriptor {
    location = local.silver_comments_path

    columns {
      name = "id"
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
      name = "body"
      type = "string"
    }
    columns {
      name = "subreddit"
      type = "string"
    }
    columns {
      name = "score"
      type = "int"
    }
    columns {
      name = "created_utc"
      type = "bigint"
    }
    columns {
      name = "is_usable_for_sentiment"
      type = "boolean"
    }
    columns {
      name = "partition_date"
      type = "string"
    }
  }

  partition_keys {
    name = "partition_date"
    type = "string"
  }
}

# =============================================================================
# Reference Data Table (Iceberg)
# =============================================================================

resource "aws_glue_catalog_table" "products_ref" {
  name          = "dim_products_ref"
  database_name = aws_glue_catalog_database.silver.name

  table_type = "ICEBERG"

  parameters = {
    "format"      = "parquet"
    "table_type"  = "ICEBERG"
  }

  storage_descriptor {
    location = local.silver_ref_data_path
    
    # All original columns from smartphones_models.csv
    columns {
      name = "brand_name"
      type = "string"
    }
    columns {
      name = "model"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "avg_rating"
      type = "double"
    }
    columns {
      name = "5G_or_not"
      type = "int"
    }
    columns {
      name = "processor_brand"
      type = "string"
    }
    columns {
      name = "num_cores"
      type = "int"
    }
    columns {
      name = "processor_speed"
      type = "double"
    }
    columns {
      name = "battery_capacity"
      type = "int"
    }
    columns {
      name = "fast_charging_available"
      type = "int"
    }
    columns {
      name = "fast_charging"
      type = "int"
    }
    columns {
      name = "ram_capacity"
      type = "int"
    }
    columns {
      name = "internal_memory"
      type = "int"
    }
    columns {
      name = "screen_size"
      type = "double"
    }
    columns {
      name = "refresh_rate"
      type = "int"
    }
    columns {
      name = "num_rear_cameras"
      type = "int"
    }
    columns {
      name = "os"
      type = "string"
    }
    columns {
      name = "primary_camera_rear"
      type = "int"
    }
    columns {
      name = "primary_camera_front"
      type = "int"
    }
    columns {
      name = "extended_memory_available"
      type = "int"
    }
    columns {
      name = "resolution_height"
      type = "int"
    }
    columns {
      name = "resolution_width"
      type = "int"
    }
    
    # Additional column for fuzzy matching (lowercase brand_name)
    columns {
      name = "brand"
      type = "string"
    }
  }
}

# =============================================================================
# Silver Sentiment Analysis Table (Iceberg)
# =============================================================================

resource "aws_glue_catalog_table" "sentiment_analysis" {
  name          = "sentiment_analysis"
  database_name = aws_glue_catalog_database.silver.name

  table_type = "ICEBERG"

  parameters = {
    "format"      = "parquet"
    "table_type"  = "ICEBERG"
  }

  storage_descriptor {
    location = local.silver_sentiment_path

    columns {
      name = "analysis_id"
      type = "string"
    }
    columns {
      name = "submission_id"
      type = "string"
    }
    columns {
      name = "comment_id"
      type = "string"
    }
    columns {
      name = "brand_name"
      type = "string"
    }
    columns {
      name = "product_name"
      type = "string"
      comment = "Specific product/model name (e.g., 'Galaxy S24 Ultra') or 'general'"
    }
    columns {
      name = "aspect"
      type = "string"
      comment = "Feature aspect (e.g., 'camera', 'battery', 'price', 'display')"
    }
    columns {
      name = "sentiment_score"
      type = "double"
      comment = "Sentiment score from 0.0 (negative) to 1.0 (positive)"
    }
    columns {
      name = "quote"
      type = "string"
      comment = "Excerpt from comment expressing this sentiment (max 200 chars)"
    }
    columns {
      name = "analyzed_at"
      type = "timestamp"
    }
    columns {
      name = "model_version"
      type = "string"
    }
    columns {
      name = "date_partition"
      type = "string"
      comment = "Partition key in DD-MM-YYYY format"
    }
  }

  partition_keys {
    name = "date_partition"
    type = "string"
  }
  
  # Note: Using single partition key for simplicity
  # Can evolve to add brand_name later via ALTER TABLE if needed
}

# =============================================================================
# Glue Job Script Upload
# =============================================================================

resource "aws_s3_object" "silver_core_script" {
  bucket = local.lakehouse_bucket_name
  key    = "glue/scripts/reddit_silver_core.py"
  source = "${path.module}/scripts/reddit_silver_core.py"
  etag   = filemd5("${path.module}/scripts/reddit_silver_core.py")

  tags = local.default_tags
}

resource "aws_s3_object" "silver_sentiment_script" {
  bucket = local.lakehouse_bucket_name
  key    = "glue/scripts/reddit_silver_sentiment.py"
  source = "${path.module}/scripts/reddit_silver_sentiment.py"
  etag   = filemd5("${path.module}/scripts/reddit_silver_sentiment.py")

  tags = local.default_tags
}

resource "aws_s3_object" "sentiment_modules" {
  bucket = local.lakehouse_bucket_name
  key    = "glue/lib/sentiment_analysis.zip"
  source = "${path.module}/build/sentiment_analysis.zip"
  etag   = filemd5("${path.module}/build/sentiment_analysis.zip")

  tags = local.default_tags
}

# =============================================================================
# Glue Job: Silver Core (Bronze → Silver Cleaning)
# =============================================================================

resource "aws_glue_job" "reddit_silver_core" {
  name     = "${local.name_prefix}-reddit-silver-core"
  role_arn = aws_iam_role.glue_silver_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  timeout = 60 # 1 hour

  command {
    name            = "glueetl"
    script_location = "s3://${local.lakehouse_bucket_name}/${aws_s3_object.silver_core_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    
    # User arguments
    "--LAKEHOUSE_BUCKET"                 = local.lakehouse_bucket_name
    "--BRONZE_DATABASE"                  = "brandrep_bronze"
    "--SILVER_DATABASE"                  = aws_glue_catalog_database.silver.name
    "--ENVIRONMENT"                      = var.environment
    "--MIN_SUBMISSION_LEN_CHARS"         = tostring(var.min_submission_len_chars)
    "--MIN_SUBMISSION_MIN_WORDS"         = tostring(var.min_submission_min_words)
    "--MIN_COMMENT_LEN_CHARS"            = tostring(var.min_comment_len_chars)
    "--MIN_COMMENT_MIN_WORDS"            = tostring(var.min_comment_min_words)
  }

  tags = local.default_tags
}

# =============================================================================
# Glue Job: Silver Sentiment Analysis
# =============================================================================

resource "aws_glue_job" "reddit_silver_sentiment" {
  name     = "${local.name_prefix}-reddit-silver-sentiment"
  role_arn = aws_iam_role.glue_silver_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 3  # Match API key count for optimal parallelism
  timeout = 60 # 1 hour

  command {
    name            = "glueetl"
    script_location = "s3://${local.lakehouse_bucket_name}/${aws_s3_object.silver_sentiment_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-job-insights"              = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "--extra-py-files"                   = "s3://${local.lakehouse_bucket_name}/${aws_s3_object.sentiment_modules.key}"
    "--additional-python-modules"        = "google-generativeai,fuzzywuzzy,python-Levenshtein"
    
    # User arguments
    "--LAKEHOUSE_BUCKET"                 = local.lakehouse_bucket_name
    "--SILVER_DATABASE"                  = aws_glue_catalog_database.silver.name
    "--GEMINI_SECRET_ARN"                = var.gemini_api_key_secret_arn
    "--ENVIRONMENT"                      = var.environment
    "--REF_DATA_S3_PATH"                 = "s3://${local.lakehouse_bucket_name}/${aws_s3_object.ref_data_csv.key}"
  }

  execution_property {
    max_concurrent_runs = 3  # Match API key count to prevent quota exhaustion
  }

  tags = local.default_tags
}
