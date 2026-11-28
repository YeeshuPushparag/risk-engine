output "glue_databases" {
  description = "Glue database names by source bucket."
  value       = { for k, db in aws_glue_catalog_database.this : k => db.name }
}

output "crawler_names" {
  description = "Glue crawler names. Start them on demand with: aws glue start-crawler --name <name>"
  value       = { for k, c in aws_glue_crawler.this : k => c.name }
}

output "glue_crawler_role_arn" {
  description = "Execution role used by all Glue crawlers."
  value       = aws_iam_role.glue_crawler.arn
}

output "athena_workgroup" {
  description = "Athena workgroup to select when running inspection queries."
  value       = aws_athena_workgroup.inspection.name
}

output "athena_results_location" {
  description = "S3 location of Athena query results."
  value       = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
}

output "athena_user_policy_arn" {
  description = "Managed policy to attach to any role or user that runs Athena queries."
  value       = aws_iam_policy.athena_user.arn
}

output "crawler_name_list" {
  description = "Flat list of crawler names, used by the Jenkins pipeline to start exactly the crawlers this project owns."
  value       = [for c in aws_glue_crawler.this : c.name]
}
