pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    TF_DIR     = "infra/ecr"
  }

  stages {

    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Terraform Init') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform init'
        }
      }
    }

    stage('Terraform Destroy') {
      steps {
        input message: 'DESTROY ECR REPOSITORIES? This will delete ALL images.', ok: 'DESTROY'
        dir(env.TF_DIR) {
          sh 'terraform destroy -auto-approve'
        }
      }
    }
  }
}
