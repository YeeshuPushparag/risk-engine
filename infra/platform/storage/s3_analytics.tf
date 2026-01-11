resource "aws_s3_bucket" "risk_analytics_data" {
  bucket = "pushparag-risk-analytics-data"

  tags = {
    Name        = "pushparag-risk-analytics-data"
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


