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

      # Terraform backend
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

      # Required for EKS IAM roles
      {
        Effect = "Allow"
        Action = [
          "iam:*"
        ]
        Resource = "*"
      },

      # EC2 / ASG describes (EKS dependency)
      {
        Effect = "Allow"
        Action = [
          "ec2:Describe*",
          "autoscaling:Describe*"
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
