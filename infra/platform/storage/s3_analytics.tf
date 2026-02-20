# =========================
# S3 Bucket for Intraday FX and Equity
# =========================

resource "aws_s3_bucket" "risk_analytics_data" {
  bucket = "pushparag-risk-analytics"

  tags = {
    Name        = "pushparag-risk-analytics"
    Project     = "risk"
    Environment = "platform"
  }
}

resource "aws_s3_bucket_versioning" "risk_analytics_data" {
  bucket = aws_s3_bucket.risk_analytics_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# =========================
# S3 Bucket for Airflow Logs
# =========================

resource "aws_s3_bucket" "airflow_logs" {
  bucket = "pushparag-airflow-logs"

  tags = {
    Name        = "pushparag-airflow-logs"
    Project     = "risk"
    Environment = "platform"
  }
}

resource "aws_s3_bucket_versioning" "airflow_logs" {
  bucket = aws_s3_bucket.airflow_logs.id

  versioning_configuration {
    status = "Enabled"
  }
}