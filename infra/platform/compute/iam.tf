# ----------------------------
# Get current AWS account ID
# ----------------------------
data "aws_caller_identity" "current" {}

# ----------------------------
# Jenkins EC2 IAM Role
# ----------------------------
resource "aws_iam_role" "jenkins_role" {
  name = "jenkins-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      },
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/pushparag"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# ----------------------------
# Jenkins IAM Policy
# ----------------------------
resource "aws_iam_role_policy" "jenkins_policy" {
  role = aws_iam_role.jenkins_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      # Terraform backend (state & locking)
      {
        Effect = "Allow"
        Action = [
          "s3:*",
          "dynamodb:*"
        ]
        Resource = "*"
      },

      # EKS creation & management
      {
        Effect = "Allow"
        Action = [
          "eks:*"
        ]
        Resource = "*"
      },

      # IAM (required for EKS, RDS monitoring roles, etc.)
      {
        Effect = "Allow"
        Action = [
          "iam:*"
        ]
        Resource = "*"
      },

      # ECR
      {
        Effect = "Allow"
        Action = "ecr:*"
        Resource = "*"
      },

      # EC2 / ASG (EKS dependency + networking)
      {
        Effect = "Allow"
        Action = [
          "ec2:*",
          "autoscaling:*"
        ]
        Resource = "*"
      },

      # RDS
      {
        Effect = "Allow"
        Action = [
          "rds:*"
        ]
        Resource = "*"
      },

      # ElastiCache
      {
        Effect = "Allow"
        Action = [
          "elasticache:*"
        ]
        Resource = "*"
      },

      # CloudWatch (logs, metrics for RDS / EKS / apps)
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:*",
          "logs:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# ----------------------------
# Jenkins Instance Profile
# ----------------------------
resource "aws_iam_instance_profile" "jenkins_profile" {
  name = "jenkins-instance-profile"
  role = aws_iam_role.jenkins_role.name
}
