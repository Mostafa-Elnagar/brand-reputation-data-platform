variable "prefix" {}
variable "name" {}
variable "owner" {}
variable "environment" {}
variable "region" {}
variable "vpc_cidr" {}
variable "ManagedBy" {}

variable "subreddits" {
  description = "List of subreddits to ingest"
  type        = list(string)
  default     = ["Smartphones", "phones", "PickAnAndroidForMe"]
}

variable "sort_types" {
  description = "List of sort types to fetch for each subreddit"
  type        = list(string)
  default     = ["top", "controversial"]
}
