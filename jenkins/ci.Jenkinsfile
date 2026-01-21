pipeline {
  agent {
    kubernetes {
      inheritFrom 'jenkins-agent'
      defaultContainer 'kaniko'
      yaml """
apiVersion: v1
kind: Pod
spec:
  nodeSelector:
    role: jenkins-agent
  containers:
  - name: kaniko
    resources:
      requests:
        cpu: "1000m"
        memory: "2Gi"
      limits:
        cpu: "2000m"
        memory: "4Gi"
  - name: jnlp
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
"""
    }
  }

  environment {
    AWS_REGION         = 'us-east-1'
    AWS_DEFAULT_REGION = 'us-east-1'
    AWS_ACCOUNT_ID     = '871007552317'
    ECR_REGISTRY       = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
    IMAGE_TAG          = "${BUILD_NUMBER}"
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Build & Push Images') {
      steps {
        container('kaniko') {
          sh '''
            set -e

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
          '''
        }
      }
    }
  }
}
