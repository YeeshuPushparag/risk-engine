pipeline {
  agent {
    kubernetes {
      label 'eks-agent'
      defaultContainer 'kaniko'
    }
  }

  environment {
    AWS_REGION          = 'us-east-1'
    AWS_DEFAULT_REGION  = 'us-east-1'
    AWS_ACCOUNT_ID      = '871007552317'
    ECR_REGISTRY        = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
    IMAGE_TAG           = "${env.BUILD_NUMBER}"
  }

  options {
    timestamps()
  }

  stages {

    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Build & Push Images (Kaniko)') {
      steps {
        container('kaniko') {
          sh '''
            set -euo pipefail

            echo "Building and pushing images to ECR..."

            /kaniko/executor \
              --context=dir://${WORKSPACE}/airflow \
              --dockerfile=${WORKSPACE}/airflow/Dockerfile \
              --destination=${ECR_REGISTRY}/airflow:${IMAGE_TAG} \
              --destination=${ECR_REGISTRY}/airflow:latest

            /kaniko/executor \
              --context=dir://${WORKSPACE}/django \
              --dockerfile=${WORKSPACE}/django/Dockerfile \
              --destination=${ECR_REGISTRY}/django:${IMAGE_TAG} \
              --destination=${ECR_REGISTRY}/django:latest

            /kaniko/executor \
              --context=dir://${WORKSPACE}/nextjs \
              --dockerfile=${WORKSPACE}/nextjs/Dockerfile \
              --destination=${ECR_REGISTRY}/nextjs:${IMAGE_TAG} \
              --destination=${ECR_REGISTRY}/nextjs:latest

            /kaniko/executor \
              --context=dir://${WORKSPACE}/spark \
              --dockerfile=${WORKSPACE}/spark/Dockerfile \
              --destination=${ECR_REGISTRY}/spark:${IMAGE_TAG} \
              --destination=${ECR_REGISTRY}/spark:latest

            /kaniko/executor \
              --context=dir://${WORKSPACE}/producers \
              --dockerfile=${WORKSPACE}/producers/Dockerfile \
              --destination=${ECR_REGISTRY}/producer:${IMAGE_TAG} \
              --destination=${ECR_REGISTRY}/producer:latest

            echo "All images built and pushed successfully."
          '''
        }
      }
    }
  }

  post {
    success {
      echo 'Kaniko build and push completed successfully.'
    }
    failure {
      echo 'Kaniko build failed.'
    }
  }
}
