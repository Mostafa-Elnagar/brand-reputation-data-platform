variable "region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (e.g., prod, dev)"
  type        = string
}

variable "prefix" {
  description = "Naming prefix"
  type        = string
  default     = "brandrep"
}

variable "name" {
  description = "Project name"
  type        = string
  default     = "market-opinion"
}

variable "ManagedBy" {
  description = "Managed by tag"
  type        = string
  default     = "Terraform"
}

variable "gemini_api_key_secret_arn" {
  description = "ARN of the secret containing the Gemini API key"
  type        = string
}

# =============================================================================
# Silver-core Cleaning Thresholds
# =============================================================================

variable "min_submission_len_chars" {
  description = "Minimum character length for submissions to be usable for sentiment analysis"
  type        = number
  default     = 10
}

variable "min_submission_min_words" {
  description = "Minimum word count for submissions to be usable for sentiment analysis"
  type        = number
  default     = 2
}

variable "min_comment_len_chars" {
  description = "Minimum character length for comments to be usable for sentiment analysis"
  type        = number
  default     = 30
}

variable "min_comment_min_words" {
  description = "Minimum word count for comments to be usable for sentiment analysis"
  type        = number
  default     = 5
}
