data "terraform_remote_state" "platform" {
  backend = "s3"

  config = {
    bucket         = "risk-tf-state-platform"
    key            = "platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
    encrypt        = true
  }
}

