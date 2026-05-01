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