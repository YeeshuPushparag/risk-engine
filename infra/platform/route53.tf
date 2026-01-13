resource "aws_route53_zone" "pushparag" {
  name = "pushparag.online"

  tags = {
    Project = "risk-engine"
  }
}


