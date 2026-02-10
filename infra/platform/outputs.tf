output "vpc_id" {
  value = module.networking.vpc_id
}

output "public_subnets" {
  value = module.networking.public_subnet_ids
}

output "private_subnet_ids" {
  value = module.networking.private_subnet_ids
}

output "risk_analytics_bucket" {
  value = module.storage.risk_analytics_bucket
}

output "acm_certificate_arn" {
  value = module.compute.acm_certificate_arn
}

output "acm_validation_records" {
  value = module.compute.acm_validation_records
}

output "route53_nameservers" {
  value = aws_route53_zone.pushparag.name_servers
}

output "route53_zone_id" {
  value = aws_route53_zone.pushparag.zone_id
}