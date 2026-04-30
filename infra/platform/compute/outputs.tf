output "jenkins_instance_id" {
  value = aws_instance.jenkins.id
}

output "jenkins_public_ip" {
  value = aws_eip.jenkins.public_ip
}

output "acm_certificate_arn" {
  value = aws_acm_certificate.main.arn
}

output "acm_validation_records" {
  value = aws_acm_certificate.main.domain_validation_options
}