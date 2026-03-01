provider "aws" {
  region = "us-east-1"
}

data "terraform_remote_state" "eks" {
  backend = "s3"

  config = {
    bucket         = "risk-tf-state-platform-pushparag"
    key            = "eks/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
  }
}