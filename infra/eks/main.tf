############################################
# EKS Cluster
############################################
resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = aws_iam_role.eks_cluster_role.arn

  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_public_access  = true
    endpoint_private_access = true
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy
  ]
}


############################################
# Node Group 1: STREAMING (Kafka, ZK, Spark)
# 1 × t3.large (FIXED)
############################################
resource "aws_eks_node_group" "streaming" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-streaming"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids

  instance_types = ["t3.large"]

  scaling_config {
    min_size     = 1
    desired_size = 1
    max_size     = 1
  }

  labels = {
    role = "streaming"
  }

  tags = {
    "k8s.io/cluster-autoscaler/enabled"              = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }
}

############################################
# Node Group 2: PLATFORM
# Airflow, Django, Next.js, ArgoCD
# 2 → 3 × t3.medium (AUTOSCALING)
############################################
resource "aws_eks_node_group" "platform" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-platform"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids

  instance_types = ["t3.medium"]

  scaling_config {
    min_size     = 2
    desired_size = 2
    max_size     = 3
  }

  labels = {
    role = "platform"
  }

  tags = {
    "k8s.io/cluster-autoscaler/enabled"              = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }
}

############################################
# Node Group 3: MONITORING
# Prometheus, Grafana (ON-DEMAND)
############################################
resource "aws_eks_node_group" "monitoring" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-monitoring"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids

  instance_types = ["t3.medium"]

  scaling_config {
    min_size     = 0
    desired_size = 0
    max_size     = 1
  }

  labels = {
    role = "monitoring"
  }

  tags = {
    "k8s.io/cluster-autoscaler/enabled"              = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }
}

############################################
# Node Group 4: JENKINS AGENTS (ON-DEMAND)
############################################
resource "aws_eks_node_group" "jenkins_agent" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "${var.cluster_name}-jenkins-agent"
  node_role_arn   = aws_iam_role.eks_node_role.arn
  subnet_ids      = var.subnet_ids

  instance_types = ["t3.large"]

  scaling_config {
    min_size     = 0
    desired_size = 0
    max_size     = 1
  }

  labels = {
    role = "jenkins-agent"
  }

  tags = {
    "k8s.io/cluster-autoscaler/enabled"              = "true"
    "k8s.io/cluster-autoscaler/${var.cluster_name}" = "owned"
  }
}

############################################
# EBS CSI Addon
############################################
resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = aws_eks_cluster.this.name
  addon_name               = "aws-ebs-csi-driver"
  service_account_role_arn = aws_iam_role.ebs_csi_irsa_role.arn

  depends_on = [
    aws_iam_role_policy_attachment.ebs_csi_attach
  ]
}
