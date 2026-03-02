############################################
# Prometheus IRSA Role (CloudWatch Access)
############################################
resource "aws_iam_role" "prometheus_irsa_role" {
  name = "${var.cluster_name}-prometheus-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"

      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      }

      Action = "sts:AssumeRoleWithWebIdentity"

      Condition = {
        StringEquals = {
          # REQUIRED by STS
          "${local.oidc_issuer}:aud" = "sts.amazonaws.com"
        }

        StringLike = {
          # Allow Prometheus SA
          "${local.oidc_issuer}:sub" = [
            "system:serviceaccount:monitoring:prometheus",

            # Allow CloudWatch exporter SA created by Helm
            "system:serviceaccount:monitoring:prometheus-cloudwatch-exporter*"
          ]
        }
      }
    }]
  })

  tags = {
    Name = "${var.cluster_name}-prometheus-irsa-role"
  }
}

############################################
# CloudWatch Read Policy
############################################
resource "aws_iam_policy" "prometheus_cloudwatch_policy" {
  name = "${var.cluster_name}-prometheus-cloudwatch-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:ListMetrics",
          "tag:GetResources"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Name = "${var.cluster_name}-prometheus-cloudwatch-policy"
  }
}

############################################
# Attach Policy to IRSA Role
############################################
resource "aws_iam_role_policy_attachment" "prometheus_cloudwatch_attach" {
  role       = aws_iam_role.prometheus_irsa_role.name
  policy_arn = aws_iam_policy.prometheus_cloudwatch_policy.arn
}