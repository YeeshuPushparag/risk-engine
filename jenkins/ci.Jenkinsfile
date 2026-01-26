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
    BUILD_IMAGES       = "false"
  }

  stages {

    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Detect App Changes (ROOT folders only)') {
      steps {
        script {
          sh '''
            set -e

            echo "Detecting app changes..."

            CHANGED=$(git diff --name-only HEAD~1 HEAD || true)

            echo "$CHANGED"

            if echo "$CHANGED" | grep -E '^(airflow|django|nextjs|spark|producers)/' >/dev/null; then
              echo "BUILD_IMAGES=true" > build.env
              echo "App code changed → will build images"
            else
              echo "BUILD_IMAGES=false" > build.env
              echo "No app code changes → skipping image build"
            fi
          '''
          def props = readProperties file: 'build.env'
          env.BUILD_IMAGES = props.BUILD_IMAGES
        }
      }
    }

    stage('Build & Push Images (Kaniko)') {
      when {
        expression { env.BUILD_IMAGES == 'true' }
      }
      steps {
        container('kaniko') {
          sh '''
            set -e
            echo "Building images with tag ${IMAGE_TAG}"

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

    stage('Update Helm Image Tags') {
      when {
        expression { env.BUILD_IMAGES == 'true' }
      }
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

              git remote set-url origin https://${GIT_USER}:${GITHUB_TOKEN}@github.com/YeeshuPushparag/risk-engine.git
              git fetch origin
              git checkout main
              git reset --hard origin/main || true

              # Update Helm image tags in main branch
              sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/*/values.yaml

              git add helm/*/values.yaml
              git commit -m "deploy: update image tags to ${IMAGE_TAG} [skip ci]" || true
              git push origin main
            '''
          }
        }
      }
    }
  }
}
