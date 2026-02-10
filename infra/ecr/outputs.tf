output "ecr_repositories" {
  value = {
    airflow       = aws_ecr_repository.airflow.repository_url
    django        = aws_ecr_repository.django.repository_url
    nextjs        = aws_ecr_repository.nextjs.repository_url
    spark         = aws_ecr_repository.spark.repository_url
    producer      = aws_ecr_repository.producer.repository_url
    kubectl       = aws_ecr_repository.kubectl.repository_url
    zookeeper     = aws_ecr_repository.zookeeper.repository_url
    kafka         = aws_ecr_repository.kafka.repository_url
    kaniko_cache  = aws_ecr_repository.kaniko_cache.repository_url
  }
}
