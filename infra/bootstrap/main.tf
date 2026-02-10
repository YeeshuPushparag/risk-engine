locals {
  project = "risk"
  env     = "platform"
}

# Terraform State Bucket
resource "aws_s3_bucket" "tf_state" {
  bucket = "${local.project}-tf-state-${local.env}-pushparag"

  tags = {
    Name        = "Terraform State Bucket"
    Project     = local.project
    Environment = local.env
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_versioning" "tf_state_versioning" {
  bucket = aws_s3_bucket.tf_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state_encrypt" {
  bucket = aws_s3_bucket.tf_state.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# DynamoDB State Lock Table
resource "aws_dynamodb_table" "tf_lock" {
  name         = "${local.project}-tf-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name        = "Terraform State Locks"
    Project     = local.project
    Environment = local.env
    ManagedBy   = "Terraform"
  }
}
