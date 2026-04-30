pipeline {
  agent any

  environment {
    AWS_REGION = "us-east-1"
    TF_VAR_cluster_name = "risk-eks"
  }

  stages {

    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    // -------------------------
    // ADDONS DESTROY FIRST
    // -------------------------
    stage('Addons Init') {
      steps {
        dir("infra/eks/addons") {
          sh 'terraform init -reconfigure'
        }
      }
    }

    stage('Addons Destroy') {
      steps {
        input message: 'Destroy EKS Addons?', ok: 'Destroy Addons'
        dir("infra/eks/addons") {

          // 🔥 CRITICAL FIX (do NOT remove)
          sh '''
          terraform destroy -auto-approve || true
          '''
        }
      }
    }

    // -------------------------
    // CLUSTER DESTROY SECOND
    // -------------------------
    stage('Cluster Init') {
      steps {
        dir("infra/eks/cluster") {
          sh 'terraform init -reconfigure'
        }
      }
    }

    stage('Cluster Destroy') {
      steps {
        input message: 'DESTROY EKS CLUSTER? This cannot be undone.', ok: 'DESTROY'
        dir("infra/eks/cluster") {
          sh 'terraform destroy -auto-approve'
        }
      }
    }
  }
}