resource "kubernetes_namespace" "jenkins" {
  metadata { name = "jenkins" }
}

resource "kubernetes_namespace" "streaming" {
  metadata { name = "streaming" }
}

resource "kubernetes_namespace" "monitoring" {
  metadata { name = "monitoring" }
}