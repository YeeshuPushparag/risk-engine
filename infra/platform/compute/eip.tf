resource "aws_eip" "jenkins" {
  domain   = "vpc"
  instance = aws_instance.jenkins.id
}