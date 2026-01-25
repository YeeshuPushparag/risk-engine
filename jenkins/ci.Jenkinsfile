pipeline {
  agent {
    kubernetes {
      inheritFrom 'jenkins-agent'
      defaultContainer 'jnlp'
    }
  }

  environment {
    AWS_REGION         = 'us-east-1'
    AWS_DEFAULT_REGION = 'us-east-1'
    AWS_ACCOUNT_ID     = '871007552317'
    ECR_REGISTRY       = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
    IMAGE_TAG          = "${BUILD_NUMBER}"
    KANIKO_CACHE_REPO  = "${ECR_REGISTRY}/kaniko-cache"
  }

  stages {

    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Build & Push Images (with cache)') {
      steps {
        container('kaniko') {
          sh '''
            set -e
            echo "Building images with tag: ${IMAGE_TAG}"
            echo "Using Kaniko cache repo: ${KANIKO_CACHE_REPO}"

            /kaniko/executor \
              --context=dir://${WORKSPACE}/airflow \
              --dockerfile=${WORKSPACE}/airflow/Dockerfile \
              --destination=${ECR_REGISTRY}/airflow:${IMAGE_TAG} \
              --cache=true \
              --cache-repo=${KANIKO_CACHE_REPO}

            /kaniko/executor \
              --context=dir://${WORKSPACE}/django \
              --dockerfile=${WORKSPACE}/django/Dockerfile \
              --destination=${ECR_REGISTRY}/django:${IMAGE_TAG} \
              --cache=true \
              --cache-repo=${KANIKO_CACHE_REPO}

            /kaniko/executor \
              --context=dir://${WORKSPACE}/nextjs \
              --dockerfile=${WORKSPACE}/nextjs/Dockerfile \
              --destination=${ECR_REGISTRY}/nextjs:${IMAGE_TAG} \
              --cache=true \
              --cache-repo=${KANIKO_CACHE_REPO}

            /kaniko/executor \
              --context=dir://${WORKSPACE}/spark \
              --dockerfile=${WORKSPACE}/spark/Dockerfile \
              --destination=${ECR_REGISTRY}/spark:${IMAGE_TAG} \
              --cache=true \
              --cache-repo=${KANIKO_CACHE_REPO}

            /kaniko/executor \
              --context=dir://${WORKSPACE}/producers \
              --dockerfile=${WORKSPACE}/producers/Dockerfile \
              --destination=${ECR_REGISTRY}/producer:${IMAGE_TAG} \
              --cache=true \
              --cache-repo=${KANIKO_CACHE_REPO}
          '''
        }
      }
    }

    stage('Update Helm Image Tags (One Commit)') {
      steps {
        // 👇 IMPORTANT: run this in container that has git (jnlp usually has it)
        container('jnlp') {
          sh '''
            set -e
            echo "Updating Helm tags to ${IMAGE_TAG}"

            sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/django/values.yaml
            sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/nextjs/values.yaml
            sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/airflow/values.yaml
            sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/streaming/values.yaml

            git status
            git add helm/*/values.yaml
            git commit -m "deploy: update image tags to ${IMAGE_TAG}" || echo "No changes"
            git push origin HEAD:main
          '''
        }
      }
    }
  }
}
