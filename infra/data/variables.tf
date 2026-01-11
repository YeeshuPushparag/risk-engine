variable "master_db_password" {
  type      = string
  sensitive = true
}

variable "airflow_db_password" {
  type      = string
  sensitive = true
}

variable "django_db_password" {
  type      = string
  sensitive = true
}
