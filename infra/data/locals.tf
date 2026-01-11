locals {
  vpc_id              = data.terraform_remote_state.platform.outputs.vpc_id
  private_subnet_ids  = data.terraform_remote_state.platform.outputs.private_subnet_ids
  name_prefix         = "risk"
}
