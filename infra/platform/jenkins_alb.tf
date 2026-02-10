# Security Group for Jenkins ALB
resource "aws_security_group" "jenkins_alb_sg" {
  name        = "jenkins-alb-sg"
  description = "ALB SG for Jenkins HTTPS"
  vpc_id      = module.networking.vpc_id

  ingress {
    description = "HTTPS"
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

  tags = {
    Project = "risk-engine"
  }
}

# ALB
resource "aws_lb" "jenkins_alb" {
  name               = "jenkins-alb"
  internal           = false
  load_balancer_type = "application"
  subnets            = module.networking.public_subnet_ids
  security_groups    = [aws_security_group.jenkins_alb_sg.id]

  enable_deletion_protection = false

  tags = {
    Project = "risk-engine"
  }
}

# Target Group
resource "aws_lb_target_group" "jenkins_tg" {
  name        = "jenkins-tg"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = module.networking.vpc_id
  target_type = "instance"

  health_check {
    path                = "/login"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 2
    matcher             = "200-399"
  }

  tags = {
    Project = "risk-engine"
  }
}

# Attach Target Group to existing Jenkins instance
resource "aws_lb_target_group_attachment" "jenkins_attachment" {
  target_group_arn = aws_lb_target_group.jenkins_tg.arn
  target_id        = module.compute.jenkins_instance_id
  port             = 8080
}

# HTTPS Listener using same ACM certificate from compute module
resource "aws_lb_listener" "jenkins_https" {
  load_balancer_arn = aws_lb.jenkins_alb.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = module.compute.acm_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.jenkins_tg.arn
  }
}
