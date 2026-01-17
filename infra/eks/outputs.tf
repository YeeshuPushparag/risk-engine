output "cluster_name" {
  value = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  value = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority" {
  value = aws_eks_cluster.this.certificate_authority[0].data
}

output "eks_node_security_group_id" {
  value = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "aws_load_balancer_controller_role_arn" {
  value = aws_iam_role.aws_load_balancer_controller.arn
}

output "airflow_s3_irsa_role_arn" {
  value = aws_iam_role.airflow_s3_irsa_role.arn
}

output "streaming_s3_irsa_role_arn" {
  value = aws_iam_role.streaming_s3_irsa_role.arn
}

output "ebs_csi_irsa_role_arn" {
  value = aws_iam_role.ebs_csi_irsa_role.arn
}
