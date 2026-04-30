terraform {
  backend "s3" {
    bucket         = "risk-tf-state-platform-yeeshu"
    key            = "platform/bootstrap/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
    encrypt        = true
  }
}
