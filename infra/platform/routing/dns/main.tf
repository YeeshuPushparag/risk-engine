provider "aws" {
  region = "us-east-1"
}

locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}"
}

resource "aws_route53_zone" "main" {
  name = "pushparag.online"

  tags = {
    Name        = "${local.name_prefix}-route53-zone"
    Project     = local.project
    Environment = local.environment
    ManagedBy  = "Terraform"
  }
}