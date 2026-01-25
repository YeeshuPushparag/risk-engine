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

stage('Update Helm Image Tags (Deploy Branch Only)') {
  steps {
    container('jnlp') {
      withCredentials([usernamePassword(
        credentialsId: 'github-https',
        usernameVariable: 'GIT_USER',
        passwordVariable: 'GITHUB_TOKEN'
      )]) {
        sh '''
          set -e

          git config user.name "Pushparag"
          git config user.email "pushparagyeeshu@gmail.com"

          # Use token auth for push
          git remote set-url origin https://${GIT_USER}:${GITHUB_TOKEN}@github.com/YeeshuPushparag/risk-engine.git

          # Fetch latest branches
          git fetch origin

          # Checkout deploy branch (create if missing)
          git checkout deploy || git checkout -b deploy

          # Make deploy branch match latest deploy from remote (if exists)
          git reset --hard origin/deploy || true

          echo "Updating Helm tags to ${IMAGE_TAG} in deploy branch..."

          sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/django/values.yaml
          sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/nextjs/values.yaml
          sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/airflow/values.yaml
          sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/streaming/values.yaml

          git add helm/*/values.yaml
          git commit -m "deploy: update image tags to ${IMAGE_TAG} [skip ci]" || echo "No changes to commit"

          # Push ONLY to deploy branch
          git push origin deploy
        '''
      }
    }
  }
}

  }
}
