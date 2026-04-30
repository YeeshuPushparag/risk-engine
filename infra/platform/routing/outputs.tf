output "route53_nameservers" {
  value = aws_route53_zone.main.name_servers
}

output "route53_zone_id" {
  value = aws_route53_zone.main.zone_id
}

output "jenkins_alb_dns" {
  value = aws_lb.jenkins.dns_name
}