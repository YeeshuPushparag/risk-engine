provider "aws" {
  region = "us-east-1"
}

locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}"
}

# DNS State
data "terraform_remote_state" "dns" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/routing/dns/terraform.tfstate"
    region = "us-east-1"
  }
}

# Discover existing Load Balancers
data "aws_lbs" "all" {}

# Identify the existing shared EKS ALB
data "aws_lb" "eks_alb" {
  name = one([
    for arn in data.aws_lbs.all.arns :
    split("/", arn)[2]
    if strcontains(split("/", arn)[2], "k8s")
  ])
}
