output "acm_certificate_arn" {
  value = aws_acm_certificate.pushparag.arn
}

output "acm_validation_records" {
  value = aws_acm_certificate.pushparag.domain_validation_options
}
