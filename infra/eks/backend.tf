terraform {
  backend "s3" {
    bucket         = "risk-tf-state-platform-yeeshu"
    key            = "eks/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
    encrypt        = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.10"
    }
  }
}
