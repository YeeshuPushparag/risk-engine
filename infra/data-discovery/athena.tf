locals {
  athena_results_bucket = coalesce(var.athena_results_bucket_name, "${var.name_prefix}-athena-results-${data.aws_caller_identity.current.account_id}")
}

# Query results live in their own bucket, separate from every source dataset.
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.athena_results_bucket
  force_destroy = var.athena_results_force_destroy
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = var.athena_results_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "athena_results_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.athena_results.arn,
      "${aws_s3_bucket.athena_results.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  policy = data.aws_iam_policy_document.athena_results_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.athena_results]
}

resource "aws_athena_workgroup" "inspection" {
  name          = "${var.name_prefix}-data-inspection"
  description   = "Troubleshooting and manual SQL inspection of Katra S3 datasets"
  state         = "ENABLED"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff_per_query
    requester_pays_enabled             = false

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  depends_on = [
    aws_s3_bucket_server_side_encryption_configuration.athena_results,
    aws_s3_bucket_policy.athena_results,
  ]
}
