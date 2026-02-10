output "acm_certificate_arn" {
  value = aws_acm_certificate.pushparag.arn
}

output "acm_validation_records" {
  value = aws_acm_certificate.pushparag.domain_validation_options
}

output "jenkins_public_ip" {
  value = aws_eip.jenkins_eip.public_ip
}


output "jenkins_instance_id" {
  value = aws_instance.jenkins.id
}
