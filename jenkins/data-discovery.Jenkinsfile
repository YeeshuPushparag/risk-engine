pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 60, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  parameters {
    choice(name: 'ACTION', choices: ['plan', 'apply'],
           description: 'plan = review only. apply = create/update. Destroy has its own pipeline.')
    booleanParam(name: 'AUTO_APPROVE', defaultValue: false,
                 description: 'Skip the manual approval prompt for apply.')
    booleanParam(name: 'RUN_CRAWLERS', defaultValue: false,
                 description: 'After a successful apply, start the crawlers owned by this Terraform project once.')
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

    stage('Tool check') {
      steps {
        sh '''
          set -e
          terraform version
          aws --version
          aws sts get-caller-identity --query Arn --output text
        '''
      }
    }

    stage('Init') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform init -input=false -no-color'
        }
      }
    }

    stage('Format check') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform fmt -check -recursive -no-color'
        }
      }
    }

    stage('Validate') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform validate -no-color'
        }
      }
    }

    stage('Plan') {
      steps {
        dir(env.TF_DIR) {
          sh 'terraform plan -input=false -no-color -out=tfplan'
          sh 'terraform show -no-color tfplan > plan.txt'
        }
        archiveArtifacts artifacts: "${env.TF_DIR}/plan.txt", allowEmptyArchive: true
      }
    }

    stage('Approval') {
      when {
        allOf {
          expression { params.ACTION == 'apply' }
          expression { !params.AUTO_APPROVE }
        }
      }
      steps {
        input message: 'Review the archived plan.txt. Proceed with terraform apply?', ok: 'Proceed'
      }
    }

    stage('Apply') {
      when {
        expression { params.ACTION == 'apply' }
      }
      steps {
        dir(env.TF_DIR) {
          sh 'terraform apply -input=false -no-color tfplan'
        }
      }
    }

    stage('Start Glue crawlers') {
      when {
        allOf {
          expression { params.ACTION == 'apply' }
          expression { params.RUN_CRAWLERS }
        }
      }
      steps {
        dir(env.TF_DIR) {
          // Names come from Terraform state, so crawlers outside this project are never touched.
          sh '''
            for c in $(terraform output -json crawler_name_list | tr -d '[]"' | tr ',' ' '); do
              echo "Starting $c"
              aws glue start-crawler --name "$c" || echo "Could not start $c (already running?)"
            done
          '''
        }
      }
    }
  }

  post {
    always {
      dir(env.TF_DIR) {
        sh 'rm -f tfplan'
      }
    }
    success {
      echo "Terraform ${params.ACTION} finished."
    }
    failure {
      echo 'Pipeline failed. Check the stage logs above.'
    }
  }
}