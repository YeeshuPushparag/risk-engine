# Comment Whole Block On First Run
# Routing / Jenkins ALB state
data "terraform_remote_state" "jenkins_routing" {
  backend = "s3"

  config = {
    bucket = "risk-tf-state-platform-katra"
    key    = "platform/routing/load-balancer/jenkins/terraform.tfstate"
    region = "us-east-1"
  }
}

resource "aws_security_group" "jenkins_sg" {
  name   = "${local.name_prefix}-jenkins-sg"
  vpc_id = data.terraform_remote_state.network.outputs.vpc_id

  ##################################
  # SSH
  ##################################
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ##################################
  # JENKINS UI 
  # Use cidr_blocks = ["0.0.0.0/0"] instead of security_groups = [] on First Run
  ##################################
  ingress {
    description     = "Jenkins UI only from ALB"
    from_port      = 8080
    to_port        = 8080
    protocol       = "tcp"
    security_groups = [
      data.terraform_remote_state.jenkins_routing.outputs.jenkins_alb_security_group_id
    ]
  }

  ##################################
  # NODE EXPORTER
  ##################################
  ingress {
    from_port   = 9100
    to_port     = 9100
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  ##################################
  # EGRESS
  ##################################
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${local.name_prefix}-jenkins-sg"
  }
}