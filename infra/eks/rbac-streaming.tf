resource "kubernetes_role" "streaming_scaler" {
  metadata {
    name      = "streaming-scaler"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "deployments/scale"]
    verbs      = ["get", "list", "watch", "patch", "update"]
  }

  depends_on = [
    kubernetes_namespace_v1.streaming
  ]
}

resource "kubernetes_role_binding" "streaming_scaler_bind" {
  metadata {
    name      = "streaming-scaler-bind"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.streaming_scaler.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = "streaming-sa"
    namespace = kubernetes_namespace_v1.streaming.metadata[0].name
  }

  depends_on = [
    kubernetes_namespace_v1.streaming,
    kubernetes_role.streaming_scaler
  ]
}
