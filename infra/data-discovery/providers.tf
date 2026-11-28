terraform {
  required_version = ">= 1.5.0"

  backend "s3" {
    bucket         = "risk-tf-state-platform-katra"
    key            = "data-discovery/glue-athena/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
    encrypt        = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}
