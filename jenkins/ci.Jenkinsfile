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

    stage('Detect App Changes') {
      steps {
        script {
          sh '''
            set -e
            echo "Detecting app changes..."
            git fetch origin

            CHANGED=$(git diff --name-only origin/main...HEAD || true)
            echo "$CHANGED"

            echo "false" > build_airflow
            echo "false" > build_django
            echo "false" > build_nextjs
            echo "false" > build_spark
            echo "false" > build_producers

            echo "$CHANGED" | grep -q '^airflow/'   && echo "true" > build_airflow   || true
            echo "$CHANGED" | grep -q '^django/'    && echo "true" > build_django    || true
            echo "$CHANGED" | grep -q '^nextjs/'    && echo "true" > build_nextjs    || true
            echo "$CHANGED" | grep -q '^spark/'     && echo "true" > build_spark     || true
            echo "$CHANGED" | grep -q '^producers/' && echo "true" > build_producers || true
          '''

          env.BUILD_AIRFLOW   = readFile('build_airflow').trim()
          env.BUILD_DJANGO    = readFile('build_django').trim()
          env.BUILD_NEXTJS    = readFile('build_nextjs').trim()
          env.BUILD_SPARK     = readFile('build_spark').trim()
          env.BUILD_PRODUCERS = readFile('build_producers').trim()

          env.BUILD_IMAGES = (
            env.BUILD_AIRFLOW   == 'true' ||
            env.BUILD_DJANGO    == 'true' ||
            env.BUILD_NEXTJS    == 'true' ||
            env.BUILD_SPARK     == 'true' ||
            env.BUILD_PRODUCERS == 'true'
          ).toString()

          echo "BUILD_AIRFLOW=${env.BUILD_AIRFLOW}"
          echo "BUILD_DJANGO=${env.BUILD_DJANGO}"
          echo "BUILD_NEXTJS=${env.BUILD_NEXTJS}"
          echo "BUILD_SPARK=${env.BUILD_SPARK}"
          echo "BUILD_PRODUCERS=${env.BUILD_PRODUCERS}"
        }
      }
    }

    stage('Build & Push Images (Kaniko)') {
      steps {
        container('kaniko') {
          script {

            if (env.BUILD_AIRFLOW == 'true') {
              sh '''
                /kaniko/executor \
                  --context=dir://${WORKSPACE}/airflow \
                  --dockerfile=${WORKSPACE}/airflow/Dockerfile \
                  --destination=${ECR_REGISTRY}/airflow:${IMAGE_TAG} \
                  --cache=true \
                  --cache-repo=${KANIKO_CACHE_REPO}
              '''
            }

            if (env.BUILD_DJANGO == 'true') {
              sh '''
                /kaniko/executor \
                  --context=dir://${WORKSPACE}/django \
                  --dockerfile=${WORKSPACE}/django/Dockerfile \
                  --destination=${ECR_REGISTRY}/django:${IMAGE_TAG} \
                  --cache=true \
                  --cache-repo=${KANIKO_CACHE_REPO}
              '''
            }

            if (env.BUILD_NEXTJS == 'true') {
              sh '''
                /kaniko/executor \
                  --context=dir://${WORKSPACE}/nextjs \
                  --dockerfile=${WORKSPACE}/nextjs/Dockerfile \
                  --destination=${ECR_REGISTRY}/nextjs:${IMAGE_TAG} \
                  --cache=true \
                  --cache-repo=${KANIKO_CACHE_REPO}
              '''
            }

            if (env.BUILD_SPARK == 'true') {
              sh '''
                /kaniko/executor \
                  --context=dir://${WORKSPACE}/spark \
                  --dockerfile=${WORKSPACE}/spark/Dockerfile \
                  --destination=${ECR_REGISTRY}/spark:${IMAGE_TAG} \
                  --cache=true \
                  --cache-repo=${KANIKO_CACHE_REPO}
              '''
            }

            if (env.BUILD_PRODUCERS == 'true') {
              sh '''
                /kaniko/executor \
                  --context=dir://${WORKSPACE}/producers \
                  --dockerfile=${WORKSPACE}/producers/Dockerfile \
                  --destination=${ECR_REGISTRY}/producer:${IMAGE_TAG} \
                  --cache=true \
                  --cache-repo=${KANIKO_CACHE_REPO}
              '''
            }

            if (env.BUILD_IMAGES != 'true') {
              echo "No app images to build."
            }
          }
        }
      }
    }

    stage('Update Helm Image Tags') {
      steps {
        script {
          if (env.BUILD_IMAGES == 'true') {
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

                  sed -i "s/^  tag: .*/  tag: \\"${IMAGE_TAG}\\"/" helm/*/values.yaml

                  git add helm/*/values.yaml
                  git commit -m "deploy: update image tags to ${IMAGE_TAG} [skip ci]" || true
                  git push origin main
                '''
              }
            }
          } else {
            echo "Skipping Helm update: no app changes detected."
          }
        }
      }
    }

  }
}
