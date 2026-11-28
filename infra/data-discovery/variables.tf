variable "aws_region" {
  description = "AWS region for all resources (must match the backend region)."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for crawler, workgroup, role and policy names. Hyphens become underscores in Glue database names."
  type        = string
  default     = "katra"
}

variable "tags" {
  description = "Default tags applied to every resource."
  type        = map(string)
  default = {
    Project   = "risk-platform-katra"
    Component = "data-discovery"
    ManagedBy = "terraform"
  }
}

variable "source_buckets" {
  description = "Existing source S3 buckets. They are only read; this project never creates or modifies them."
  type = object({
    equity      = string
    fx          = string
    bond        = string
    commodity   = string
    loan        = string
    collateral  = string
    derivatives = string
    platform    = string
  })
  default = {
    equity      = "katra-equity-bucket"
    fx          = "katra-fx-bucket"
    bond        = "katra-bond-bucket"
    commodity   = "katra-commodity-bucket"
    loan        = "katra-loan-bucket"
    collateral  = "katra-collateral-bucket"
    derivatives = "katra-derivatives-bucket"
    platform    = "risk-platform-katra-analytics"
  }
}

variable "catalog_root_files" {
  description = "Catalog the single files that sit directly in bucket/prefix roots (reference CSV/Parquet, loan datasets, latest.json) using single-file crawler targets. Set to false if the verification step in the README shows Glue does not create correct single-file tables."
  type        = bool
  default     = true
}

variable "recrawl_behavior" {
  description = "Recrawl policy for partitioned datasets. CRAWL_NEW_FOLDERS_ONLY skips folders already crawled (schema changes are only logged). CRAWL_EVERYTHING re-reads everything and updates table schemas."
  type        = string
  default     = "CRAWL_NEW_FOLDERS_ONLY"

  validation {
    condition     = contains(["CRAWL_NEW_FOLDERS_ONLY", "CRAWL_EVERYTHING"], var.recrawl_behavior)
    error_message = "recrawl_behavior must be CRAWL_NEW_FOLDERS_ONLY or CRAWL_EVERYTHING."
  }
}

variable "crawler_sample_size" {
  description = "Files sampled per leaf folder by partitioned-dataset crawlers (1-249). Parquet schemas are embedded and consistent, so 1 keeps crawls fast and cheap."
  type        = number
  default     = 1

  validation {
    condition     = var.crawler_sample_size >= 1 && var.crawler_sample_size <= 249
    error_message = "crawler_sample_size must be between 1 and 249."
  }
}

variable "crawler_schedule" {
  description = "Optional cron expression applied to every crawler, for example cron(0 2 * * ? *). Null keeps crawlers on-demand, which is the cheapest option."
  type        = string
  default     = null
}

variable "athena_results_bucket_name" {
  description = "Name of the new S3 bucket for Athena query results. Null generates <name_prefix>-athena-results-<account id>."
  type        = string
  default     = null
}

variable "athena_results_retention_days" {
  description = "Days before Athena query results expire."
  type        = number
  default     = 30
}

variable "athena_results_force_destroy" {
  description = "Allow terraform destroy to delete the results bucket even when it contains query results."
  type        = bool
  default     = true
}

variable "athena_bytes_scanned_cutoff_per_query" {
  description = "Per-query data scan limit in bytes (minimum 10485760). Default is 10 GiB."
  type        = number
  default     = 10737418240

  validation {
    condition     = var.athena_bytes_scanned_cutoff_per_query >= 10485760
    error_message = "Athena requires a scan cutoff of at least 10485760 bytes (10 MB)."
  }
}

variable "athena_user_role_names" {
  description = "Optional existing IAM role names that should receive the Athena inspection policy. Leave empty to attach the policy yourself."
  type        = list(string)
  default     = []
}
