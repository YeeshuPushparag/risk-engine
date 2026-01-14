data "aws_lb" "risk_engine_alb" {
  dns_name = "k8s-riskengine-ca22df56e5-778505082.us-east-1.elb.amazonaws.com"
}

resource "aws_route53_record" "app" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "app.pushparag.online"
  type    = "A"

  alias {
    name                   = data.aws_lb.risk_engine_alb.dns_name
    zone_id                = data.aws_lb.risk_engine_alb.zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "api" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "api.pushparag.online"
  type    = "A"

  alias {
    name                   = data.aws_lb.risk_engine_alb.dns_name
    zone_id                = data.aws_lb.risk_engine_alb.zone_id
    evaluate_target_health = true
  }
}
