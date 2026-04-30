locals {
  vpc_id         = data.terraform_remote_state.platform_network.outputs.vpc_id
  eks_subnet_ids = data.terraform_remote_state.platform_network.outputs.public_subnet_ids
}