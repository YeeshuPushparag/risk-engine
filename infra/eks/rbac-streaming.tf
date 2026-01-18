resource "kubernetes_role" "streaming_scaler" {
  metadata {
    name      = "streaming-scaler"
    namespace = "streaming"
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments"]
    verbs      = ["get", "list", "watch", "patch", "update"]
  }
}

resource "kubernetes_role_binding" "streaming_scaler_bind" {
  metadata {
    name      = "streaming-scaler-bind"
    namespace = "streaming"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.streaming_scaler.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = "streaming-sa"
    namespace = "streaming"
  }
}
