locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}-pushparag"
}

# -------------------------
# Risk Analytics Bucket
# -------------------------
resource "aws_s3_bucket" "risk_analytics" {
  bucket = "${local.name_prefix}-analytics"

  tags = {
    Name        = "${local.name_prefix}-analytics"
    Project     = local.project
    Environment = local.environment
  }
}

resource "aws_s3_bucket_versioning" "risk_analytics" {
  bucket = aws_s3_bucket.risk_analytics.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -------------------------
# Airflow Logs Bucket
# -------------------------
resource "aws_s3_bucket" "airflow_logs" {
  bucket = "${local.name_prefix}-airflow-logs"

  tags = {
    Name        = "${local.name_prefix}-airflow-logs"
    Project     = local.project
    Environment = local.environment
  }
}

resource "aws_s3_bucket_versioning" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id

  versioning_configuration {
    status = "Enabled"
  }
}