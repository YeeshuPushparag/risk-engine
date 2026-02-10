provider "aws" {
  region = "us-east-1"
}

data "terraform_remote_state" "platform" {
  backend = "s3"
  config = {
    bucket         = "risk-tf-state-platform"
    key            = "platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
  }
}

data "terraform_remote_state" "eks" {
  backend = "s3"
  config = {
    bucket         = "risk-tf-state-platform"
    key            = "eks/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
  }
}


provider "postgresql" {
  host     = aws_db_instance.postgres.address
  port     = 5432
  database = "postgres"
  username = aws_db_instance.postgres.username
  password = var.master_db_password
  sslmode  = "require"
}
