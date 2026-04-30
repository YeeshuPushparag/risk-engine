# -------------------------
# Remote State: Compute
# -------------------------
data "terraform_remote_state" "compute" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-yeeshu"
    key    = "platform/compute/terraform.tfstate"
    region = "us-east-1"
  }
}

# -------------------------
# Remote State: Networking
# -------------------------
data "terraform_remote_state" "network" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-yeeshu"
    key    = "platform/networking/terraform.tfstate"
    region = "us-east-1"
  }
}

# -------------------------
# Remote State: EKS (cluster only)
# -------------------------
data "terraform_remote_state" "eks" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-yeeshu"
    key    = "eks/cluster/terraform.tfstate"   # ✅ FIXED
    region = "us-east-1"
  }
}

locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}"
}

# -------------------------
# Route53 Hosted Zone
# -------------------------
resource "aws_route53_zone" "main" {
  name = "pushparag.online"

  tags = {
    Project = local.project
  }
}

# -------------------------
# ALB Security Group
# -------------------------
resource "aws_security_group" "alb_sg" {
  name   = "${local.name_prefix}-alb-sg"
  vpc_id = data.terraform_remote_state.network.outputs.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# -------------------------
# Jenkins ALB
# -------------------------
resource "aws_lb" "jenkins" {
  name               = "jenkins-alb"
  load_balancer_type = "application"
  internal           = false

  subnets         = data.terraform_remote_state.network.outputs.public_subnet_ids
  security_groups = [aws_security_group.alb_sg.id]
}

# -------------------------
# Target Group
# -------------------------
resource "aws_lb_target_group" "jenkins" {
  name        = "jenkins-tg"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = data.terraform_remote_state.network.outputs.vpc_id
  target_type = "instance"

  health_check {
    path = "/login"
  }
}

# -------------------------
# Attach Jenkins EC2
# -------------------------
resource "aws_lb_target_group_attachment" "jenkins" {
  target_group_arn = aws_lb_target_group.jenkins.arn
  target_id        = data.terraform_remote_state.compute.outputs.jenkins_instance_id
  port             = 8080
}

# -------------------------
# HTTPS Listener
# -------------------------
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.jenkins.arn
  port              = 443
  protocol          = "HTTPS"

  certificate_arn = data.terraform_remote_state.compute.outputs.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.jenkins.arn
  }
}

# -------------------------
# EKS ALB Lookup (NOW HERE, NOT IN ADDONS)
# -------------------------
data "aws_lb" "eks_alb" {
  most_recent = true

  filter {
    name   = "tag:elbv2.k8s.aws/cluster"
    values = [data.terraform_remote_state.eks.outputs.cluster_name]
  }
}

# -------------------------
# Route53: Jenkins
# -------------------------
resource "aws_route53_record" "jenkins" {
  zone_id = aws_route53_zone.main.zone_id
  name    = "jenkins.pushparag.online"
  type    = "A"

  alias {
    name                   = aws_lb.jenkins.dns_name
    zone_id                = aws_lb.jenkins.zone_id
    evaluate_target_health = true
  }
}

# -------------------------
# Route53: EKS wildcard
# -------------------------
resource "aws_route53_record" "wildcard" {
  zone_id = aws_route53_zone.main.zone_id
  name    = "*.pushparag.online"
  type    = "A"

  alias {
    name                   = data.aws_lb.eks_alb.dns_name
    zone_id                = data.aws_lb.eks_alb.zone_id
    evaluate_target_health = true
  }
}