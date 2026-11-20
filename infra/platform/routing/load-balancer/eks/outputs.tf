output "eks_alb_arn" {
  description = "ARN of the shared EKS ALB"
  value       = data.aws_lb.eks_alb.arn
}

output "eks_alb_dns_name" {
  description = "DNS name of the shared EKS ALB"
  value       = data.aws_lb.eks_alb.dns_name
}

output "eks_alb_zone_id" {
  description = "Canonical hosted zone ID of the shared EKS ALB"
  value       = data.aws_lb.eks_alb.zone_id
}

output "eks_wildcard_dns" {
  description = "Wildcard DNS for EKS applications"
  value       = aws_route53_record.eks_wildcard.fqdn
}