resource "aws_db_subnet_group" "postgres" {
  name       = "${local.name_prefix}-postgres-subnets"
  subnet_ids = local.private_subnet_ids
}

resource "aws_db_instance" "postgres" {
  identifier             = "${local.name_prefix}-postgres"
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = "db.t4g.micro"

  allocated_storage      = 20
  max_allocated_storage  = 50

  db_name                = "postgres"
  username               = "masteruser"
  password               = var.master_db_password

  db_subnet_group_name   = aws_db_subnet_group.postgres.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  publicly_accessible    = false
  deletion_protection    = false
  skip_final_snapshot    = true
}
