variable "instance_type" {
  default = "t3.medium"
}

variable "jenkins_key_name" {
  description = "EC2 Key Pair Name to SSH"
  type        = string
}

variable "name_prefix" {
  description = "Prefix for resource naming"
  type        = string
}

variable "vpc_id" {
  type = string
}

variable "public_subnets" {
  type = list(string)
}
