pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 60, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  environment {
    // Credentials come from the Jenkins EC2 instance profile. No keys are stored here.
    TF_DIR             = 'infra/data-discovery'
    TF_IN_AUTOMATION   = 'true'
    TF_INPUT           = '0'
    AWS_DEFAULT_REGION = 'us-east-1'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Init') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform init -input=false -no-color'
        }
      }
    }

    stage('Plan destroy') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform plan -destroy -input=false -no-color -out=tfdestroy'
          sh 'terraform show -no-color tfdestroy > destroy-plan.txt'
        }
        archiveArtifacts artifacts: "${env.TF_DIR}/destroy-plan.txt", allowEmptyArchive: true
      }
    }

    stage('Confirm destroy') {
      steps {
        script {
          def answer = input(
            message: 'Review destroy-plan.txt. This removes the Glue databases, crawlers, IAM roles, Athena workgroup and the query-results bucket. Source buckets are not touched.',
            parameters: [string(name: 'CONFIRM', defaultValue: '', description: 'Type destroy to continue')]
          )
          if (answer != 'destroy') {
            error('Destroy not confirmed.')
          }
        }
      }
    }

    stage('Destroy') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform apply -input=false -no-color tfdestroy'
        }
      }
    }
  }

  post {
    always {
      dir(env.TF_DIR) {
        sh 'rm -f tfdestroy'
      }
    }
  }
}
