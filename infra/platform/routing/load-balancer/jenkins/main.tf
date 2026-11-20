provider "aws" {
  region = "us-east-1"
}

locals {
  project      = "risk"
  environment  = "platform"
  name_prefix  = "${local.project}-${local.environment}"

  jenkins_name = "jenkins"
}

# Networking state
data "terraform_remote_state" "networking" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/networking/terraform.tfstate"
    region = "us-east-1"
  }
}

# Compute state
data "terraform_remote_state" "compute" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/compute/terraform.tfstate"
    region = "us-east-1"
  }
}

# DNS state
data "terraform_remote_state" "dns" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/routing/dns/terraform.tfstate"
    region = "us-east-1"
  }
}

# TLS state
data "terraform_remote_state" "tls" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/routing/tls/terraform.tfstate"
    region = "us-east-1"
  }
}

# ALB Security Group
resource "aws_security_group" "jenkins_alb" {
  name        = "${local.name_prefix}-jenkins-alb-sg"
  description = "Security group for Jenkins Application Load Balancer"
  vpc_id    = data.terraform_remote_state.networking.outputs.vpc_id

  ingress {
    description = "HTTP from internet"
    from_port  = 80
    to_port    = 80
    protocol   = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS from internet"
    from_port  = 443
    to_port    = 443
    protocol   = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "All outbound traffic"
    from_port  = 0
    to_port    = 0
    protocol   = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${local.name_prefix}-jenkins-alb-sg"
    Project     = local.project
    Environment = local.environment
    ManagedBy  = "Terraform"
  }
}

# Jenkins Application Load Balancer
resource "aws_lb" "jenkins" {
  name               = "${local.name_prefix}-jenkins-alb"
  internal            = false
  load_balancer_type = "application"
  security_groups     = [aws_security_group.jenkins_alb.id]

  subnets = data.terraform_remote_state.networking.outputs.public_subnet_ids

  enable_deletion_protection = true

  tags = {
    Name        = "${local.name_prefix}-jenkins-alb"
    Project     = local.project
    Environment = local.environment
    ManagedBy  = "Terraform"
  }
}

# Jenkins Target Group
resource "aws_lb_target_group" "jenkins" {
  name        = "${local.name_prefix}-jenkins-tg"
  port        = 8080
  protocol    = "HTTP"
  vpc_id    = data.terraform_remote_state.networking.outputs.vpc_id
  target_type = "instance"

  health_check {
    enabled             = true
    protocol            = "HTTP"
    path               = "/login"
    port               = "8080"
    matcher             = "200-399"
    interval            = 30
    timeout            = 5
    healthy_threshold   = 3
    unhealthy_threshold = 3
  }

  tags = {
    Name        = "${local.name_prefix}-jenkins-target-group"
    Project     = local.project
    Environment = local.environment
    ManagedBy  = "Terraform"
  }
}

# Attach Jenkins EC2 to Target Group
resource "aws_lb_target_group_attachment" "jenkins" {
  target_group_arn = aws_lb_target_group.jenkins.arn
  target_id        = data.terraform_remote_state.compute.outputs.jenkins_instance_id
  port            = 8080
}

# HTTP listener - redirect to HTTPS
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.jenkins.arn
  port              = 80
  protocol           = "HTTP"

  default_action {
    type = "redirect"

    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

# HTTPS listener - forward to Jenkins
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.jenkins.arn
  port              = 443
  protocol           = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  certificate_arn = data.terraform_remote_state.tls.outputs.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.jenkins.arn
  }
}