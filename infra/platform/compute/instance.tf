resource "aws_instance" "jenkins" {
  ami                         = data.aws_ami.amazon_linux_2023.id
  instance_type               = var.instance_type
  subnet_id                   = var.public_subnets[0]
  associate_public_ip_address = true

  key_name               = var.jenkins_key_name
  iam_instance_profile   = aws_iam_instance_profile.jenkins_profile.name
  vpc_security_group_ids = [aws_security_group.jenkins_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
  }

  user_data = <<-EOF
    #!/bin/bash
    dnf update -y
    dnf install -y java-17-amazon-corretto wget
    wget -O /etc/yum.repos.d/jenkins.repo https://pkg.jenkins.io/redhat-stable/jenkins.repo
    rpm --import https://pkg.jenkins.io/redhat-stable/jenkins.io-2023.key
    dnf install -y jenkins
    systemctl enable jenkins
    systemctl start jenkins
  EOF

  tags = {
    Name = "${var.name_prefix}-jenkins"
  }
  lifecycle {
    ignore_changes = [ami]
  }
}
