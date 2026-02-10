locals {
  oidc_issuer = replace(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
  eks_subnet_ids = data.terraform_remote_state.platform.outputs.private_subnet_ids
  vpc_id         = data.terraform_remote_state.platform.outputs.vpc_id
}
