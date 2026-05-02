data "terraform_remote_state" "network" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-yeeshu"
    key    = "platform/networking/terraform.tfstate"
    region = "us-east-1"
  }
}

data "aws_caller_identity" "current" {}

data "aws_ssm_parameter" "amazon_linux" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "image-id"
    values = [data.aws_ssm_parameter.amazon_linux.value]
  }
}