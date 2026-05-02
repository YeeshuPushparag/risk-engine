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
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:user/yeeshu"
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

      {
        Effect = "Allow"
        Action = ["s3:*", "dynamodb:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["eks:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["iam:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["ecr:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["ec2:*", "autoscaling:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["rds:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["elasticache:*"]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = ["cloudwatch:*", "logs:*"]
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