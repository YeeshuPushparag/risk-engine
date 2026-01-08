variable "cluster_name" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "core_instance_type" {
  type    = string
  default = "t3.large"
}

variable "compute_instance_type" {
  type    = string
  default = "t3.large"
}

variable "compute_max_size" {
  type    = number
  default = 2
}
