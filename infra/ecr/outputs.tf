output "ecr_repositories" {
  value = {
    airflow  = aws_ecr_repository.airflow.repository_url
    django   = aws_ecr_repository.django.repository_url
    nextjs   = aws_ecr_repository.nextjs.repository_url
    spark    = aws_ecr_repository.spark.repository_url
    producer = aws_ecr_repository.producer.repository_url
  }
}
