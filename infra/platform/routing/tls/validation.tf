# Keep it empty on first tls terraform apply 
locals {
  acm_validation_records = distinct([
    for dvo in aws_acm_certificate.main.domain_validation_options : {
      name  = dvo.resource_record_name
      type  = dvo.resource_record_type
      value = dvo.resource_record_value
    }
  ])
}

resource "aws_route53_record" "acm_validation" {
  for_each = {
    for record in local.acm_validation_records :
    record.name => record
  }

  zone_id = data.terraform_remote_state.dns.outputs.route53_zone_id
  name    = each.value.name
  type    = each.value.type
  ttl    = 60
  records = [each.value.value]

  allow_overwrite = true
}

resource "aws_acm_certificate_validation" "main" {
  certificate_arn = aws_acm_certificate.main.arn

  validation_record_fqdns = [
    for record in aws_route53_record.acm_validation :
    record.fqdn
  ]
}