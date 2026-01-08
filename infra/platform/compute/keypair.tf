resource "aws_key_pair" "jenkins" {
  key_name   = "risk-jenkins-key"
  public_key = file("${path.module}/risk-jenkins-key.pub")
}
