variable "name_prefix" {
  type = string
}

variable "jenkins_key_name" {
  type = string
}

variable "instance_type" {
  type    = string
  default = "t3.medium"
}

variable "vpc_id" {
  type = string
}

variable "public_subnets" {
  type = list(string)
}
