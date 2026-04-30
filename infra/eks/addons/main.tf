# -------------------------
# aws-auth (READ EXISTING)
# -------------------------
data "kubernetes_config_map_v1" "aws_auth" {
  metadata {
    name      = "aws-auth"
    namespace = "kube-system"
  }
}

# -------------------------
# aws-auth (PATCH)
# -------------------------
resource "kubernetes_config_map_v1" "aws_auth_patch" {
  metadata {
    name      = "aws-auth"
    namespace = "kube-system"
  }

  data = {
    mapRoles = yamlencode([
      {
        rolearn  = data.terraform_remote_state.cluster.outputs.node_role_arn
        username = "system:node:{{EC2PrivateDNSName}}"
        groups   = [
          "system:bootstrappers",
          "system:nodes"
        ]
      }
    ])
  }

  depends_on = [
    data.kubernetes_config_map_v1.aws_auth
  ]
}

# -------------------------
# EBS CSI Addon
# -------------------------
resource "aws_eks_addon" "ebs_csi" {
  cluster_name = data.terraform_remote_state.cluster.outputs.cluster_name
  addon_name   = "aws-ebs-csi-driver"

  depends_on = [
    kubernetes_config_map_v1.aws_auth_patch
  ]
}