output "jenkins_alb_arn" {
  description = "Jenkins Application Load Balancer ARN"
  value       = aws_lb.jenkins.arn
}

output "jenkins_alb_dns_name" {
  description = "AWS DNS name of Jenkins ALB"
  value       = aws_lb.jenkins.dns_name
}

output "jenkins_url" {
  description = "Jenkins HTTPS URL"
  value       = "https://jenkins.pushparag.online"
}

output "jenkins_target_group_arn" {
  description = "Jenkins target group ARN"
  value       = aws_lb_target_group.jenkins.arn
}

output "jenkins_alb_security_group_id" {
  description = "Security group ID attached to Jenkins ALB"
  value       = aws_security_group.jenkins_alb.id
}