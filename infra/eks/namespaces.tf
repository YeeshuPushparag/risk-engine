resource "kubernetes_namespace" "jenkins" {
  metadata {
    name = "jenkins"
  }
}

resource "kubernetes_namespace_v1" "streaming" {
  metadata {
    name = "streaming"
  }
}
