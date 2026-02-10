terraform {
  backend "s3" {
    bucket         = "risk-tf-state-platform-pushparag"
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
  }
}
