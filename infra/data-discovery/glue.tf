resource "aws_glue_catalog_database" "this" {
  for_each = local.glue_databases

  name        = each.value.name
  description = each.value.description
}

# Crawlers own schema and partition discovery. Terraform deliberately does not
# manage Glue tables, so there is no state drift between the two.
resource "aws_glue_crawler" "this" {
  for_each = local.crawlers

  name          = "${var.name_prefix}-${each.key}"
  description   = each.value.description
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.this[each.value.database].name
  table_prefix  = each.value.table_prefix == "" ? null : each.value.table_prefix
  schedule      = each.value.scheduled ? var.crawler_schedule : null

  dynamic "s3_target" {
    for_each = each.value.targets

    content {
      path        = s3_target.value.path
      exclusions  = s3_target.value.exclusions
      sample_size = s3_target.value.sample_size
    }
  }

  recrawl_policy {
    recrawl_behavior = each.value.recrawl
  }

  # CRAWL_NEW_FOLDERS_ONLY requires LOG/LOG. Tables are never deleted by a crawler.
  schema_change_policy {
    update_behavior = each.value.recrawl == "CRAWL_NEW_FOLDERS_ONLY" ? "LOG" : "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  # New partitions inherit the table schema, which avoids partition schema
  # mismatch errors in Athena when a later file has extra or reordered columns.
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  depends_on = [aws_iam_role_policy.glue_crawler]
}