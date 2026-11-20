output "route53_zone_id" {
  description = "Route53 hosted zone ID"
  value       = aws_route53_zone.main.zone_id
}

output "route53_zone_name" {
  description = "Route53 hosted zone domain name"
  value       = aws_route53_zone.main.name
}

output "route53_nameservers" {
  description = "Authoritative nameservers assigned to the hosted zone"
  value       = aws_route53_zone.main.name_servers
}