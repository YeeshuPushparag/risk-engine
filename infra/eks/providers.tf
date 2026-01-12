provider "aws" {
  region = "us-east-1"
}

data "terraform_remote_state" "data" {
  backend = "s3"
  config = {
    bucket         = "risk-tf-state-platform"
    key            = "data/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
  }
}
