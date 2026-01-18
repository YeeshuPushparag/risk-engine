# jenkins-irsa-ecr.tf

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# Get EKS cluster info (must already exist in your terraform)
data "aws_eks_cluster" "this" {
  name = aws_eks_cluster.this.name
}

# OIDC provider (must already exist in your terraform)
# If you already have aws_iam_openid_connect_provider.eks, reuse it.
# If not, you must create it (but you said it's already there).
data "aws_iam_openid_connect_provider" "eks" {
  arn = aws_iam_openid_connect_provider.eks.arn
}

# -------------------------------------------------------
# IAM Policy for ECR push/pull (Kaniko)
# -------------------------------------------------------
resource "aws_iam_policy" "jenkins_ecr_policy" {
  name        = "risk-eks-jenkins-ecr-policy"
  description = "Allow Jenkins Kaniko agents to push/pull images to ECR"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Needed for docker/kaniko login to ECR
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },

      # Needed to push/pull images
      {
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
# IRSA Role for ServiceAccount jenkins-agent
# -------------------------------------------------------
resource "aws_iam_role" "jenkins_irsa_role" {
  name = "risk-eks-jenkins-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = data.aws_iam_openid_connect_provider.eks.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            # IMPORTANT: This must match your EKS OIDC issuer
            "${replace(data.aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")}:sub" = "system:serviceaccount:jenkins:jenkins-agent"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "jenkins_ecr_attach" {
  role       = aws_iam_role.jenkins_irsa_role.name
  policy_arn = aws_iam_policy.jenkins_ecr_policy.arn
}

# -------------------------------------------------------
# Attach IRSA role to ServiceAccount
# -------------------------------------------------------
resource "kubernetes_service_account_v1" "jenkins_agent_irsa_patch" {
  metadata {
    name      = kubernetes_service_account.jenkins_agent.metadata[0].name
    namespace = "jenkins"

    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.jenkins_irsa_role.arn
    }
  }

  depends_on = [
    kubernetes_service_account.jenkins_agent
  ]
}
