pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    TF_DIR     = "infra/data"

    TF_VAR_master_db_password  = credentials('rds-master-password')
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

    stage('Terraform Validate') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform validate'
        }
      }
    }

    stage('Terraform Plan') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform plan -out=tfplan'
        }
      }
    }

    stage('Terraform Apply') {
      steps {
        input message: 'Apply Terraform to create/update RDS & ElastiCache?', ok: 'Apply'
        dir(env.TF_DIR) {
          sh 'terraform apply -input=false tfplan'
        }
      }
    }
  }
}
