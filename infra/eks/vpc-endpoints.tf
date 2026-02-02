############################################
# Security Group for STS VPC Endpoint
############################################
resource "aws_security_group" "sts_vpce_sg" {
  name        = "${var.cluster_name}-sts-vpce-sg"
  description = "Allow EKS pods to reach STS via VPC endpoint"
  vpc_id      = var.vpc_id

  ingress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [
      aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
    ]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

############################################
# STS Interface VPC Endpoint
############################################
resource "aws_vpc_endpoint" "sts" {
  vpc_id            = var.vpc_id
  service_name      = "com.amazonaws.us-east-1.sts"
  vpc_endpoint_type = "Interface"

  subnet_ids = var.subnet_ids

  security_group_ids = [
    aws_security_group.sts_vpce_sg.id,
    aws_security_group.jenkins_sg.id
  ]

  private_dns_enabled = true
}
