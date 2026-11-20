terraform {
  backend "s3" {
    bucket       = "risk-tf-state-platform-katra"
    key          = "platform/routing/load-balancer/eks/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}