pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    TF_DIR     = "infra/eks"

    TF_VAR_cluster_name = "risk-eks"
    TF_VAR_vpc_id       = "vpc-0d61e23e46f2284e3"
    TF_VAR_subnet_ids   = '["subnet-0599001b2af627f97","subnet-087699aef4b60c2ff"]'
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
