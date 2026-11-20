resource "aws_route53_record" "jenkins" {
  zone_id = data.terraform_remote_state.dns.outputs.route53_zone_id
  name    = "jenkins.pushparag.online"
  type    = "A"

  alias {
    name                   = aws_lb.jenkins.dns_name
    zone_id                = aws_lb.jenkins.zone_id
    evaluate_target_health  = true
  }
}