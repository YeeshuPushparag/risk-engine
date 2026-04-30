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

locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}"
}

# -------------------------
# AMI
# -------------------------
data "aws_ssm_parameter" "amazon_linux" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "image-id"
    values = [data.aws_ssm_parameter.amazon_linux.value]
  }
}

# -------------------------
# Key Pair
# -------------------------
resource "aws_key_pair" "jenkins" {
  key_name   = "risk-jenkins-key"
  public_key = file("${path.module}/risk-jenkins-key.pub")
}

# -------------------------
# IAM
# -------------------------
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "jenkins_role" {
  name = "jenkins-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "jenkins_policy" {
  role = aws_iam_role.jenkins_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["*"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "jenkins" {
  name = "jenkins-instance-profile"
  role = aws_iam_role.jenkins_role.name
}

# -------------------------
# Security Group
# -------------------------
resource "aws_security_group" "jenkins_sg" {
  name   = "${local.name_prefix}-jenkins-sg"
  vpc_id = data.terraform_remote_state.network.outputs.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
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
# EC2 Jenkins (FIXED USER_DATA)
# -------------------------
resource "aws_instance" "jenkins" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.instance_type
  subnet_id                   = data.terraform_remote_state.network.outputs.public_subnet_ids[0]
  associate_public_ip_address = true

  key_name               = aws_key_pair.jenkins.key_name
  iam_instance_profile   = aws_iam_instance_profile.jenkins.name
  vpc_security_group_ids = [aws_security_group.jenkins_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e

    dnf update -y

    # Java
    dnf install -y java-21-amazon-corretto

    # Required dependency (IMPORTANT FIX)
    dnf install -y fontconfig

    # Jenkins repo
    wget -O /etc/yum.repos.d/jenkins.repo https://pkg.jenkins.io/redhat-stable/jenkins.repo
    rpm --import https://pkg.jenkins.io/redhat-stable/jenkins.io-2023.key

    # Install Jenkins
    dnf install -y jenkins

    # Fix Java path issue (CRITICAL FIX)
    echo 'JENKINS_JAVA_CMD="/usr/bin/java"' >> /etc/sysconfig/jenkins

    systemctl daemon-reexec
    systemctl enable jenkins
    systemctl start jenkins
  EOF

  tags = {
    Name = "${local.name_prefix}-jenkins"
  }

  lifecycle {
    ignore_changes = [ami]
  }
}

# -------------------------
# Elastic IP
# -------------------------
resource "aws_eip" "jenkins" {
  domain   = "vpc"
  instance = aws_instance.jenkins.id
}

# -------------------------
# ACM
# -------------------------
resource "aws_acm_certificate" "main" {
  domain_name               = "pushparag.online"
  subject_alternative_names = ["*.pushparag.online"]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}