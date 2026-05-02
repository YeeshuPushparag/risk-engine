resource "aws_instance" "jenkins" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = var.instance_type
  subnet_id                   = data.terraform_remote_state.network.outputs.public_subnet_ids[0]
  associate_public_ip_address = true

  key_name = aws_key_pair.jenkins.key_name

  # IMPORTANT — matches your IAM resource name
  iam_instance_profile = aws_iam_instance_profile.jenkins_profile.name

  vpc_security_group_ids = [aws_security_group.jenkins_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    set -e

    dnf update -y
    dnf install -y java-21-amazon-corretto fontconfig

    wget -O /etc/yum.repos.d/jenkins.repo https://pkg.jenkins.io/redhat-stable/jenkins.repo
    rpm --import https://pkg.jenkins.io/redhat-stable/jenkins.io-2023.key

    dnf install -y jenkins

    echo 'JENKINS_JAVA_CMD="/usr/bin/java"' >> /etc/sysconfig/jenkins

    systemctl daemon-reexec
    systemctl enable jenkins
    systemctl start jenkins
  EOF

  tags = {
    Name = "${local.name_prefix}-jenkins"
  }

  lifecycle {
    ignore_changes = [ami, user_data]
  }
}