############################################
# EKS CLUSTER
############################################

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = aws_iam_role.eks_cluster_role.arn

  vpc_config {
    subnet_ids = var.subnet_ids
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy
  ]
}

############################################
# READ DATA STACK (RDS / REDIS)
############################################

data "terraform_remote_state" "data" {
  backend = "s3"
  config = {
    bucket         = "risk-tf-state-platform"
    key            = "data/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "risk-tf-locks"
  }
}

############################################
# CORE NODE GROUP
############################################

resource "aws_eks_node_group" "core" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-core"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids

  instance_types = [var.core_instance_type]

  scaling_config {
    min_size     = 0
    desired_size = 0
    max_size     = var.core_max_size
  }

  labels = {
    role = "core"
  }

  additional_security_group_ids = [
    data.terraform_remote_state.data.outputs.app_access_sg_id
  ]
}

############################################
# COMPUTE NODE GROUP
############################################

resource "aws_eks_node_group" "compute" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-compute"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids

  instance_types = [var.compute_instance_type]

  scaling_config {
    min_size     = 0
    desired_size = 0
    max_size     = var.compute_max_size
  }

  labels = {
    role = "compute"
  }

  additional_security_group_ids = [
    data.terraform_remote_state.data.outputs.app_access_sg_id
  ]
}
