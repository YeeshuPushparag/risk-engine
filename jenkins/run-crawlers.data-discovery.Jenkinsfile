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

    stage('Start Crawlers') {
      steps {
        dir(env.TF_DIR) {
          // Names come from Terraform state, so only this project's 24 crawlers are started.
          sh '''
            terraform output -json crawler_name_list | tr -d '[]"' | tr ',' '\\n' > crawlers.txt
            for c in $(cat crawlers.txt); do
              echo "Starting $c"
              aws glue start-crawler --name "$c" || echo "Could not start $c (already running?)"
            done
          '''
        }
      }
    }

    stage('Wait and Check Results') {
      steps {
        dir(env.TF_DIR) {
          sh '''
            for i in $(seq 1 60); do
              running=0
              for c in $(cat crawlers.txt); do
                state=$(aws glue get-crawler --name "$c" --query Crawler.State --output text)
                [ "$state" != "READY" ] && running=$((running+1))
              done
              [ "$running" -eq 0 ] && break
              echo "$running crawler(s) still running..."
              sleep 30
            done

            failed=0
            for c in $(cat crawlers.txt); do
              status=$(aws glue get-crawler --name "$c" --query Crawler.LastCrawl.Status --output text)
              echo "$c: $status"
              [ "$status" = "FAILED" ] && failed=1
            done
            exit $failed
          '''
        }
      }
    }
  }
}
