pipeline {
  agent {
    kubernetes {
      inheritFrom 'jenkins-agent'
      defaultContainer 'jnlp'
    }
  }

  environment {
    AWS_REGION     = 'us-east-1'
  }

  stages {
 stage('Resolve AWS Account') {
      steps {
        container('jnlp') {
          script {
            // Get AWS account ID from your credentials
            env.AWS_ACCOUNT_ID = sh(
              script: "aws sts get-caller-identity --query Account --output text",
              returnStdout: true
            ).trim()

            // Build ECR variables
            env.ECR_REGISTRY = "${env.AWS_ACCOUNT_ID}.dkr.ecr.${env.AWS_REGION}.amazonaws.com"
            env.KANIKO_CACHE = "${env.ECR_REGISTRY}/kaniko-cache"
          }
        }
      }
    }


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
            git fetch origin

            CHANGED=$(git diff --name-only "$GIT_PREVIOUS_SUCCESSFUL_COMMIT" "$GIT_COMMIT" || true)

            echo false > build_airflow
            echo false > build_django
            echo false > build_nextjs
            echo false > build_spark
            echo false > build_producer

            echo "$CHANGED" | grep -q '^airflow/'   && echo true > build_airflow   || true
            echo "$CHANGED" | grep -q '^django/'    && echo true > build_django    || true
            echo "$CHANGED" | grep -q '^nextjs/'    && echo true > build_nextjs    || true
            echo "$CHANGED" | grep -q '^spark/'     && echo true > build_spark     || true
            echo "$CHANGED" | grep -q '^producer/'  && echo true > build_producer  || true
          '''

          env.BUILD_AIRFLOW  = readFile('build_airflow').trim()
          env.BUILD_DJANGO   = readFile('build_django').trim()
          env.BUILD_NEXTJS   = readFile('build_nextjs').trim()
          env.BUILD_SPARK    = readFile('build_spark').trim()
          env.BUILD_PRODUCER = readFile('build_producer').trim()

          env.BUILD_IMAGES = (
            env.BUILD_AIRFLOW  == 'true' ||
            env.BUILD_DJANGO   == 'true' ||
            env.BUILD_NEXTJS   == 'true' ||
            env.BUILD_SPARK    == 'true' ||
            env.BUILD_PRODUCER == 'true'
          ).toString()
        }
      }
    }

    stage('Build & Push Images') {
      when {
        expression { env.BUILD_IMAGES == 'true' }
      }
      steps {
        container('kaniko') {
          script {

            // READ ENV ONCE (sandbox-safe)
            def buildFlags = [
              airflow  : env.BUILD_AIRFLOW,
              django   : env.BUILD_DJANGO,
              nextjs   : env.BUILD_NEXTJS,
              spark    : env.BUILD_SPARK,
              producer : env.BUILD_PRODUCER
            ]

            def builds = [
              [id: 'airflow',  name: 'airflow',  path: 'airflow',  values: 'helm/airflow/values.yaml',  key: null],
              [id: 'django',   name: 'django',   path: 'django',   values: 'helm/django/values.yaml',   key: null],
              [id: 'nextjs',   name: 'nextjs',   path: 'nextjs',   values: 'helm/nextjs/values.yaml',   key: null],
              [id: 'producer', name: 'producer', path: 'producer', values: 'helm/streaming/values.yaml', key: 'producer'],
              [id: 'spark',    name: 'spark',    path: 'spark',    values: 'helm/streaming/values.yaml', key: 'spark']
            ]

            for (b in builds) {
              if (buildFlags[b.id] == 'true') {

                sh """
                  set -e

                  VALUES_FILE="${b.values}"

                  if [ "${b.key}" = "" ] || [ "${b.key}" = "null" ]; then
                    CURRENT_TAG=\$(grep '^  tag:' \$VALUES_FILE | awk '{print \$2}' | tr -d '"')
                  else
                    CURRENT_TAG=\$(awk '/${b.key}:/{f=1} f && /tag:/{print \$2; exit}' \$VALUES_FILE | tr -d '"')
                  fi

                  NEXT_TAG=\$((CURRENT_TAG + 1))

                  /kaniko/executor \
                    --context=dir://${WORKSPACE}/${b.path} \
                    --dockerfile=${WORKSPACE}/${b.path}/Dockerfile \
                    --destination=${ECR_REGISTRY}/${b.name}:\$NEXT_TAG \
                    --cache=true \
                    --cache-repo=${KANIKO_CACHE}

                  if [ "${b.key}" = "" ] || [ "${b.key}" = "null" ]; then
                    sed -i "s/^  tag:.*/  tag: \\"\$NEXT_TAG\\"/" \$VALUES_FILE
                  else
                    sed -i "/${b.key}:/,/tag:/ s/tag:.*/tag: \\"\$NEXT_TAG\\"/" \$VALUES_FILE
                  fi
                """
              }
            }
          }
        }
      }
    }

    stage('Commit Helm Changes') {
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
              git checkout main
              git pull origin main

              git add helm
              git commit -m "deploy: bump image tags [skip ci]" || true
              git push origin main
            '''
          }
        }
      }
    }
  }
}
