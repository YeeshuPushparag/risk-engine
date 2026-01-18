pipeline {
  agent {
    kubernetes {
      label 'eks-agent'
      defaultContainer 'kaniko'
    }
  }

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

    stage("Build & Push Images (Kaniko)") {
      steps {
        sh """
          set -e

          /kaniko/executor --context ${WORKSPACE}/airflow   --dockerfile ${WORKSPACE}/airflow/Dockerfile   --destination ${ECR_REGISTRY}/airflow:latest
          /kaniko/executor --context ${WORKSPACE}/django    --dockerfile ${WORKSPACE}/django/Dockerfile    --destination ${ECR_REGISTRY}/django:latest
          /kaniko/executor --context ${WORKSPACE}/nextjs    --dockerfile ${WORKSPACE}/nextjs/Dockerfile    --destination ${ECR_REGISTRY}/nextjs:latest
          /kaniko/executor --context ${WORKSPACE}/spark     --dockerfile ${WORKSPACE}/spark/Dockerfile     --destination ${ECR_REGISTRY}/spark:latest
          /kaniko/executor --context ${WORKSPACE}/producers --dockerfile ${WORKSPACE}/producers/Dockerfile --destination ${ECR_REGISTRY}/producer:latest
        """
      }
    }
  }
}
