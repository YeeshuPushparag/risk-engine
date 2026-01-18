# jenkins-irsa-ecr.tf

# -------------------------------------------------------
# Namespace for Jenkins agents
# -------------------------------------------------------
resource "kubernetes_namespace" "jenkins" {
  metadata {
    name = "jenkins"
  }
}

# -------------------------------------------------------
# IAM Policy: Allow Jenkins (Kaniko) to push/pull from ECR
# -------------------------------------------------------
resource "aws_iam_policy" "jenkins_ecr" {
  name        = "risk-eks-jenkins-ecr-policy"
  description = "ECR push/pull permissions for Jenkins Kaniko agents via IRSA"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRAuth"
        Effect = "Allow"
        Action = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Sid    = "ECRPushPull"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:PutImage",
          "ecr:UploadLayerPart",
          "ecr:DescribeRepositories",
          "ecr:ListImages"
        ]
        Resource = "*"
      }
    ]
  })
}

# -------------------------------------------------------
# IAM Role: IRSA role for jenkins-agent ServiceAccount
# -------------------------------------------------------
resource "aws_iam_role" "jenkins_irsa" {
  name = "risk-eks-jenkins-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.eks.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${replace(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")}:sub" = "system:serviceaccount:jenkins:jenkins-agent"
            "${replace(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")}:aud" = "sts.amazonaws.com"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "jenkins_ecr" {
  role       = aws_iam_role.jenkins_irsa.name
  policy_arn = aws_iam_policy.jenkins_ecr.arn
}

# -------------------------------------------------------
# Kubernetes ServiceAccount (ONLY ONE) with IRSA annotation
# -------------------------------------------------------
resource "kubernetes_service_account_v1" "jenkins_agent" {
  metadata {
    name      = "jenkins-agent"
    namespace = kubernetes_namespace.jenkins.metadata[0].name

    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.jenkins_irsa.arn
    }
  }
}
