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

# Discover existing Kubernetes ALBs
data "aws_lbs" "all" {}

# Identify the shared EKS ALB
data "aws_lb" "eks_alb" {
  name = one([
    for name in data.aws_lbs.all.names :
    name if strcontains(name, "k8s")
  ])
}
