pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    TF_DIR     = "infra/data-discovery"
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
          sh 'terraform init -reconfigure'
        }
      }
    }

    stage('Terraform Plan Destroy') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform plan -destroy -out=tfdestroy'
        }
      }
    }

    stage('Terraform Destroy') {
      steps {
        input message: 'Destroy Glue crawlers, databases, Athena workgroup, IAM roles and the query-results bucket? Source buckets are not touched.', ok: 'Destroy'
        dir(env.TF_DIR) {
          sh 'terraform apply -input=false tfdestroy'
        }
      }
    }
  }
}
