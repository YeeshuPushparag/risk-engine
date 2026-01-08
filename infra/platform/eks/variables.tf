variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "subnet_ids" {
  description = "Subnets for EKS"
  type        = list(string)
}

variable "core_instance_type" {
  description = "Instance type for core node group"
  type        = string
  default     = "t3.large"
}

variable "compute_instance_type" {
  description = "Instance type for compute node group"
  type        = string
  default     = "t3.large"
}

variable "compute_max_size" {
  description = "Maximum compute nodes"
  type        = number
  default     = 2
}
