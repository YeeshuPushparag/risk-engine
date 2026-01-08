variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "risk-eks"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID where EKS will be created"
  type        = string
}

variable "subnet_ids" {
  description = "Subnets for EKS worker nodes"
  type        = list(string)
}
