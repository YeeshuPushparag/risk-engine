terraform {
  backend "s3" {
    bucket         = "risk-tf-state-platform"
    key            = "platform/bootstrap/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
    encrypt        = true
  }
}
