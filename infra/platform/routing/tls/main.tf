provider "aws" {
  region = "us-east-1"
}

locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}"
}

data "terraform_remote_state" "dns" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/routing/dns/terraform.tfstate"
    region = "us-east-1"
  }
}

resource "aws_acm_certificate" "main" {
  domain_name               = "pushparag.online"
  subject_alternative_names  = ["*.pushparag.online"]
  validation_method          = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name        = "${local.name_prefix}-acm-certificate"
    Project     = local.project
    Environment = local.environment
    ManagedBy  = "Terraform"
  }
}