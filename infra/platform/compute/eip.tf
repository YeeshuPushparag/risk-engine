resource "aws_eip" "jenkins_eip" {
  domain = "vpc"

  instance = aws_instance.jenkins.id

  tags = {
    Name = "${var.name_prefix}-jenkins-eip"
  }
}
