resource "aws_acm_certificate" "pushparag" {
  domain_name               = "pushparag.online"
  subject_alternative_names = ["*.pushparag.online"]
  validation_method         = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Project = "risk-engine"
  }
}

output "acm_certificate_arn" {
  value = aws_acm_certificate.pushparag.arn
}

output "acm_validation_records" {
  value = aws_acm_certificate.pushparag.domain_validation_options
}
