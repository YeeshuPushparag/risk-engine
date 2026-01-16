resource "aws_route53_zone" "pushparag" {
  name = "pushparag.online"

  tags = {
    Project = "risk-engine"
  }
}

# ------------------------------------------------------------
# Route53 Records -> ALB (EKS Ingress)
# ------------------------------------------------------------
locals {
  alb_dns_name = "k8s-riskengine-ca22df56e5-1341215229.us-east-1.elb.amazonaws.com"
  alb_zone_id  = "Z35SXDOTRQ7X7K" # ALB Hosted Zone ID for us-east-1
}

resource "aws_route53_record" "app" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "app.pushparag.online"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}

resource "aws_route53_record" "api" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "api.pushparag.online"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}


resource "aws_route53_record" "airflow" {
  zone_id = aws_route53_zone.pushparag.zone_id
  name    = "airflow.pushparag.online"
  type    = "A"

  alias {
    name                   = local.alb_dns_name
    zone_id                = local.alb_zone_id
    evaluate_target_health = true
  }
}

