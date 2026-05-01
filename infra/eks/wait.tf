resource "time_sleep" "wait_for_cluster" {
  create_duration = "30s"

  triggers = {
    cluster_endpoint = aws_eks_cluster.this.endpoint
  }

  depends_on = [aws_eks_cluster.this]
}