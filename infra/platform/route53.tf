resource "aws_route53_zone" "pushparag" {
  name = "pushparag.online"

  tags = {
    Project = "risk-engine"
  }
}

# ------------------------------------------------------------
# Auto-discover the EKS ALB created by AWS Load Balancer Controller
# ------------------------------------------------------------
data "aws_lb" "eks_alb" {
  tags = {
    "elbv2.k8s.aws/cluster" = "risk-eks"
  }
}

# ------------------------------------------------------------
# Wildcard record -> routes ALL subdomains to same ALB
# ------------------------------------------------------------
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
