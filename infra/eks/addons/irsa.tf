data "aws_iam_openid_connect_provider" "eks" {
  arn = data.terraform_remote_state.cluster.outputs.oidc_provider_arn
}

# Example: Jenkins IRSA
resource "aws_iam_role" "jenkins_irsa" {
  name = "risk-eks-jenkins-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = data.aws_iam_openid_connect_provider.eks.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
    }]
  })
}