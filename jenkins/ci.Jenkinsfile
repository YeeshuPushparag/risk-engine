pipeline {
  agent {
    kubernetes {
      inheritFrom 'jenkins-agent'
      defaultContainer 'jnlp'
    }
  }

  environment {
    AWS_REGION = 'us-east-1'
  }

  stages {

    stage('Resolve AWS Account') {
      steps {
        container('jnlp') {
          script {
            env.AWS_ACCOUNT_ID = sh(
              script: "echo $AWS_ROLE_ARN | cut -d: -f5",
              returnStdout: true
            ).trim()

            env.ECR_REGISTRY = "${env.AWS_ACCOUNT_ID}.dkr.ecr.${env.AWS_REGION}.amazonaws.com"
            env.KANIKO_CACHE = "${env.ECR_REGISTRY}/kaniko-cache"
          }
        }
      }
    }

    // -------------------------
    // CHECKOUT
    // -------------------------
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    // -------------------------
    // DETECT CHANGES (FIXED)
    // -------------------------
    stage('Detect App Changes') {
      steps {
        container('jnlp') {
          script {

            sh 'git fetch origin'

            def changed = sh(
              script: 'git diff --name-only "$GIT_PREVIOUS_SUCCESSFUL_COMMIT" "$GIT_COMMIT" || true',
              returnStdout: true
            ).trim()

            echo "Changed files:\n${changed}"

            env.BUILD_AIRFLOW  = ((changed =~ /(^|\n)airflow\//).find())  ? 'true' : 'false'
            env.BUILD_DJANGO   = ((changed =~ /(^|\n)django\//).find())   ? 'true' : 'false'
            env.BUILD_NEXTJS   = ((changed =~ /(^|\n)nextjs\//).find())   ? 'true' : 'false'
            env.BUILD_SPARK    = ((changed =~ /(^|\n)spark\//).find())    ? 'true' : 'false'
            env.BUILD_PRODUCER = ((changed =~ /(^|\n)producer\//).find()) ? 'true' : 'false'

            echo "BUILD_AIRFLOW=${env.BUILD_AIRFLOW}"
            echo "BUILD_DJANGO=${env.BUILD_DJANGO}"
            echo "BUILD_NEXTJS=${env.BUILD_NEXTJS}"
            echo "BUILD_SPARK=${env.BUILD_SPARK}"
            echo "BUILD_PRODUCER=${env.BUILD_PRODUCER}"

            env.BUILD_IMAGES = (
              env.BUILD_AIRFLOW  == 'true' ||
              env.BUILD_DJANGO   == 'true' ||
              env.BUILD_NEXTJS   == 'true' ||
              env.BUILD_SPARK    == 'true' ||
              env.BUILD_PRODUCER == 'true'
            ).toString()

            echo "BUILD_IMAGES=${env.BUILD_IMAGES}"
          }
        }
      }
    }

    // -------------------------
    // BUILD & PUSH (SELECTIVE)
    // -------------------------
    stage('Build & Push Images') {
      when {
        expression { env.BUILD_IMAGES == 'true' }
      }
      steps {
        container('kaniko') {
          script {

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

    // -------------------------
    // COMMIT CHANGES
    // -------------------------
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