pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    AWS_ACCOUNT_ID = "871007552317"
    ECR_REGISTRY = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
  }

  stages {
    stage("Checkout") {
      steps {
        checkout scm
      }
    }

    stage("ECR Login") {
      steps {
        sh """
          aws ecr get-login-password --region ${AWS_REGION} \
          | docker login --username AWS --password-stdin ${ECR_REGISTRY}
        """
      }
    }

    stage("Build Images") {
      steps {
        sh """
          docker build -t airflow:latest   -f airflow/Dockerfile   airflow
          docker build -t django:latest    -f django/Dockerfile    django
          docker build -t nextjs:latest    -f nextjs/Dockerfile    nextjs
          docker build -t spark:latest     -f spark/Dockerfile     spark
          docker build -t producer:latest  -f producers/Dockerfile producers
        """
      }
    }

    stage("Tag Images") {
      steps {
        sh """
          docker tag airflow:latest   ${ECR_REGISTRY}/airflow:latest
          docker tag django:latest    ${ECR_REGISTRY}/django:latest
          docker tag nextjs:latest    ${ECR_REGISTRY}/nextjs:latest
          docker tag spark:latest     ${ECR_REGISTRY}/spark:latest
          docker tag producer:latest  ${ECR_REGISTRY}/producer:latest
        """
      }
    }

    stage("Push Images to ECR") {
      steps {
        sh """
          docker push ${ECR_REGISTRY}/airflow:latest
          docker push ${ECR_REGISTRY}/django:latest
          docker push ${ECR_REGISTRY}/nextjs:latest
          docker push ${ECR_REGISTRY}/spark:latest
          docker push ${ECR_REGISTRY}/producer:latest
        """
      }
    }
  }
}
