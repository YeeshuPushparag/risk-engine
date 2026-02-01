output "risk_analytics_bucket" {
  value = aws_s3_bucket.risk_analytics_data.bucket
}

output "airflow_logs_bucket" {
  value = aws_s3_bucket.airflow_logs.bucket
}