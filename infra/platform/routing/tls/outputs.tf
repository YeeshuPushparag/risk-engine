output "acm_certificate_arn" {
  description = "ACM certificate ARN for pushparag.online and its wildcard"
  value       = aws_acm_certificate.main.arn
}

output "acm_certificate_domains" {
  description = "Domains covered by the ACM certificate"
  value       = aws_acm_certificate.main.domain_name
}

output "acm_certificate_status" {
  description = "Current ACM certificate status"
  value       = aws_acm_certificate.main.status
}