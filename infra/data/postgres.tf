resource "postgresql_role" "airflow" {
  name     = "airflow_user"
  login    = true
  password = var.airflow_db_password
}

resource "postgresql_role" "django" {
  name     = "django_user"
  login    = true
  password = var.django_db_password
}

resource "postgresql_database" "airflow" {
  name  = "airflow_db"
  owner = postgresql_role.airflow.name
}

resource "postgresql_database" "django" {
  name  = "django_db"
  owner = postgresql_role.django.name
}
