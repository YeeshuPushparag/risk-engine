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

    stage('Terraform Destroy') {
      steps {
        input message: 'DESTROY RDS & ElastiCache? This is irreversible.', ok: 'Destroy'
        dir(env.TF_DIR) {
          sh 'terraform destroy -auto-approve'
        }
      }
    }
  }
}
