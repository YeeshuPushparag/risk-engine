resource "aws_key_pair" "jenkins" {
  key_name   = var.jenkins_key_name
  public_key = file("${path.module}/risk-jenkins-key.pub")
}
