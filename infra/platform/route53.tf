resource "aws_route53_zone" "pushparag" {
  name = "pushparag.online"

  tags = {
    Project = "risk-engine"
  }
}



# Jenkins record pointing to It's ALB
resource "aws_route53_record" "jenkins" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "jenkins.pushparag.online"
  type    = "A"

  alias {
    name                   = aws_lb.jenkins_alb.dns_name
    zone_id                = aws_lb.jenkins_alb.zone_id
    evaluate_target_health = true
  }
}

# EKS wildcard record
data "aws_lb" "eks_alb" {
  tags = {
    "elbv2.k8s.aws/cluster" = "risk-eks"
  }
}

resource "aws_route53_record" "wildcard" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "*.pushparag.online"
  type    = "A"

  alias {
    name                   = data.aws_lb.eks_alb.dns_name
    zone_id                = data.aws_lb.eks_alb.zone_id
    evaluate_target_health = true
  }
}