provider "aws" {
  region = "us-east-1"
}

data "terraform_remote_state" "cluster" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-yeeshu"
    key    = "eks/cluster/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "kubernetes" {
  host = data.terraform_remote_state.cluster.outputs.cluster_endpoint

  cluster_ca_certificate = base64decode(
    data.terraform_remote_state.cluster.outputs.cluster_certificate_authority
  )

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args = [
      "eks",
      "get-token",
      "--cluster-name",
      data.terraform_remote_state.cluster.outputs.cluster_name
    ]
  }
}