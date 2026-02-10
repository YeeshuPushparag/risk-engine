pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    TF_DIR     = "infra/eks"

    TF_VAR_cluster_name = "risk-eks"
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
        input message: 'DESTROY EKS CLUSTER? This cannot be undone.', ok: 'DESTROY'
        dir(env.TF_DIR) {
          sh 'terraform destroy -auto-approve'
        }
      }
    }
  }
}
