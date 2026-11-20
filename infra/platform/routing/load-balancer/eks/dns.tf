# Wildcard DNS for EKS applications
resource "aws_route53_record" "eks_wildcard" {
  zone_id = data.terraform_remote_state.dns.outputs.route53_zone_id
  name    = "*.pushparag.online"
  type    = "A"

  alias {
    name                   = data.aws_lb.eks_alb.dns_name
    zone_id                = data.aws_lb.eks_alb.zone_id
    evaluate_target_health  = true
  }
}