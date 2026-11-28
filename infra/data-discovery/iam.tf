locals {
  partition  = data.aws_partition.current.partition
  account_id = data.aws_caller_identity.current.account_id

  source_bucket_arns  = [for name in values(var.source_buckets) : "arn:${local.partition}:s3:::${name}"]
  source_object_arns  = [for arn in local.source_bucket_arns : "${arn}/*"]
  glue_catalog_arn    = "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:catalog"
  glue_database_arns  = [for db in aws_glue_catalog_database.this : db.arn]
  glue_table_arns     = [for db in aws_glue_catalog_database.this : "arn:${local.partition}:glue:${var.aws_region}:${local.account_id}:table/${db.name}/*"]
  glue_catalog_scoped = concat([local.glue_catalog_arn], local.glue_database_arns, local.glue_table_arns)
}

# -----------------------------------------------------------------------------
# Glue crawler execution role (assumed by the Glue service, not by Jenkins)
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "GlueServiceAssumeRole"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_crawler" {
  name               = "${var.name_prefix}-glue-crawler-role"
  description        = "Execution role for Glue crawlers that catalog the Katra S3 datasets"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

data "aws_iam_policy_document" "glue_crawler" {
  statement {
    sid       = "ReadSourceObjects"
    actions   = ["s3:GetObject"]
    resources = local.source_object_arns
  }

  statement {
    sid       = "ListSourceBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = local.source_bucket_arns
  }

  statement {
    sid = "WriteCatalogMetadata"
    actions = [
      "glue:Get*",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:BatchUpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition",
      "glue:BatchGetPartition",
    ]
    resources = local.glue_catalog_scoped
  }

  statement {
    sid       = "ReadClassifiers"
    actions   = ["glue:GetClassifier", "glue:GetClassifiers"]
    resources = ["*"]
  }

  statement {
    sid     = "CrawlerLogs"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:${local.partition}:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_crawler" {
  name   = "${var.name_prefix}-glue-crawler-policy"
  role   = aws_iam_role.glue_crawler.id
  policy = data.aws_iam_policy_document.glue_crawler.json
}

# -----------------------------------------------------------------------------
# Athena runtime permissions. Athena runs with the permissions of whoever submits
# the query, so this managed policy is for the people or roles that run queries.
# -----------------------------------------------------------------------------
data "aws_iam_policy_document" "athena_user" {
  statement {
    sid = "UseInspectionWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:BatchGetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetQueryResultsStream",
      "athena:ListQueryExecutions",
      "athena:GetWorkGroup",
      "athena:CreateNamedQuery",
      "athena:GetNamedQuery",
      "athena:ListNamedQueries",
    ]
    resources = [aws_athena_workgroup.inspection.arn]
  }

  statement {
    sid = "BrowseAthenaCatalog"
    actions = [
      "athena:ListWorkGroups",
      "athena:ListDataCatalogs",
      "athena:ListDatabases",
      "athena:GetDatabase",
      "athena:ListTableMetadata",
      "athena:GetTableMetadata",
    ]
    resources = ["*"]
  }

  statement {
    sid = "ReadGlueCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:BatchGetPartition",
    ]
    resources = local.glue_catalog_scoped
  }

  statement {
    sid       = "ReadSourceObjects"
    actions   = ["s3:GetObject"]
    resources = local.source_object_arns
  }

  statement {
    sid       = "ListSourceBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = local.source_bucket_arns
  }

  statement {
    sid = "ReadWriteQueryResults"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.athena_results.arn}/*"]
  }

  statement {
    sid = "ListQueryResultsBucket"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.athena_results.arn]
  }
}

resource "aws_iam_policy" "athena_user" {
  name        = "${var.name_prefix}-athena-inspection-user"
  description = "Run Athena queries in the inspection workgroup against the Katra Glue catalog"
  policy      = data.aws_iam_policy_document.athena_user.json
}

resource "aws_iam_role_policy_attachment" "athena_user" {
  for_each = toset(var.athena_user_role_names)

  role       = each.value
  policy_arn = aws_iam_policy.athena_user.arn
}
