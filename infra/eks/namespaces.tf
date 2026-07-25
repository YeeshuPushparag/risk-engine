# Namespace resources
resource "kubernetes_namespace" "airflow" {
  metadata { name = "airflow" }
  depends_on = [aws_eks_node_group.platform_core] 
}

resource "kubernetes_namespace" "nextjs" {
  metadata { name = "nextjs" }
  depends_on = [aws_eks_node_group.web] 
}

resource "kubernetes_namespace" "django" {
  metadata { name = "django" }
  depends_on = [aws_eks_node_group.web] 
}

resource "kubernetes_namespace" "jenkins" {
  metadata { name = "jenkins" }
  depends_on = [aws_eks_node_group.platform_core] 
}

resource "kubernetes_namespace_v1" "streaming" {
  metadata { name = "streaming" }
  depends_on = [aws_eks_node_group.streaming]      
}

resource "kubernetes_namespace_v1" "monitoring" {
  metadata { name = "monitoring" }
  depends_on = [aws_eks_node_group.monitoring]       
}