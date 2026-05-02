variable "project" {
  type    = string
  default = "risk"
}

variable "environment" {
  type    = string
  default = "platform"
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}