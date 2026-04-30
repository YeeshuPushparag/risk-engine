data "terraform_remote_state" "platform_network" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-yeeshu"
    key    = "platform/networking/terraform.tfstate"
    region = "us-east-1"
  }
}