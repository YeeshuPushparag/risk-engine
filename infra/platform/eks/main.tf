resource "aws_eks_cluster" "this" {
  name     = local.cluster_name
  role_arn = aws_iam_role.eks_cluster_role.arn

  vpc_config {
    subnet_ids = local.subnet_ids
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy
  ]
}

# ----------------------------
# CORE NODE GROUP (always on)
# ----------------------------
resource "aws_eks_node_group" "core" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${local.cluster_name}-core"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = local.subnet_ids

  instance_types = [var.core_instance_type]

  scaling_config {
    min_size     = 1
    desired_size = 1
    max_size     = 1
  }

  labels = {
    role = "core"
  }

  depends_on = [
    aws_eks_cluster.this
  ]
}

# ----------------------------
# COMPUTE NODE GROUP (autoscale)
# ----------------------------
resource "aws_eks_node_group" "compute" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${local.cluster_name}-compute"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = local.subnet_ids

  instance_types = [var.compute_instance_type]

  scaling_config {
    min_size     = 0
    desired_size = 0
    max_size     = var.compute_max_size
  }

  labels = {
    role = "compute"
  }

  depends_on = [
    aws_eks_cluster.this
  ]
}
