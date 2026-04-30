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
    // EKS CLUSTER
    // -------------------------
    stage('Cluster Init') {
      steps {
        dir("infra/eks/cluster") {
          sh 'terraform init -reconfigure'
        }
      }
    }

    stage('Cluster Validate') {
      steps {
        dir("infra/eks/cluster") {
          sh 'terraform validate'
        }
      }
    }

    stage('Cluster Plan') {
      steps {
        dir("infra/eks/cluster") {
          sh 'terraform plan'
        }
      }
    }

    stage('Cluster Apply') {
      steps {
        input message: 'Apply EKS Cluster?', ok: 'Apply'
        dir("infra/eks/cluster") {
          sh 'terraform apply -auto-approve'
        }
      }
    }

    // -------------------------
    // EKS ADDONS
    // -------------------------
    stage('Addons Init') {
      steps {
        dir("infra/eks/addons") {
          sh 'terraform init -reconfigure'
        }
      }
    }

    stage('Addons Validate') {
      steps {
        dir("infra/eks/addons") {
          sh 'terraform validate'
        }
      }
    }

    stage('Addons Apply') {
      steps {
        input message: 'Apply EKS Addons?', ok: 'Apply'
        dir("infra/eks/addons") {

          // 🔥 CRITICAL: take ownership before apply
          sh '''
          terraform import kubernetes_config_map_v1.aws_auth_patch kube-system/aws-auth || true
          '''

          // 🔥 apply directly (NO tfplan)
          sh 'terraform apply -auto-approve'
        }
      }
    }
  }
}