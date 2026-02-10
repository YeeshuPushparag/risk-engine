############################################
# IRSA Role: Streaming (namespace: streaming)
# Used by: spark + producers (same ServiceAccount)
############################################

resource "aws_iam_role" "streaming_s3_irsa_role" {
  name = "${var.cluster_name}-streaming-s3-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Federated = aws_iam_openid_connect_provider.eks.arn
        },
        Action = "sts:AssumeRoleWithWebIdentity",
        Condition = {
          StringEquals = {
            "${local.oidc_issuer}:sub" = "system:serviceaccount:streaming:streaming-sa"
          }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "streaming_s3_policy" {
  name = "${var.cluster_name}-streaming-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "StreamingS3Access",
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "streaming_s3_attach" {
  role       = aws_iam_role.streaming_s3_irsa_role.name
  policy_arn = aws_iam_policy.streaming_s3_policy.arn
}

############################################
# IRSA Role: Airflow (namespace: airflow)
############################################
resource "aws_iam_role" "airflow_s3_irsa_role" {
  name = "${var.cluster_name}-airflow-s3-irsa-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      },
      Action = "sts:AssumeRoleWithWebIdentity",
      Condition = {
        StringEquals = {
          "${local.oidc_issuer}:sub" = "system:serviceaccount:airflow:airflow-sa"
        }
      }
    }]
  })
}

resource "aws_iam_policy" "airflow_s3_policy" {
  name = "${var.cluster_name}-airflow-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Sid    = "AirflowS3Access",
      Effect = "Allow",
      Action = [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "airflow_s3_attach" {
  role       = aws_iam_role.airflow_s3_irsa_role.name
  policy_arn = aws_iam_policy.airflow_s3_policy.arn
}


