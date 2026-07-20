############################################
# AIRFLOW RBAC
# KubernetesPodOperator
############################################

resource "kubernetes_role" "airflow_operator" {
  metadata {
    name      = "airflow-operator"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  rule {
    api_groups = [""]

    resources = [
      "pods",
      "pods/log",
      "pods/status"
    ]

    verbs = [
      "create",
      "delete",
      "deletecollection",
      "get",
      "list",
      "watch",
      "patch",
      "update"
    ]
  }

  depends_on = [
    kubernetes_namespace.airflow
  ]
}

resource "kubernetes_role_binding" "airflow_operator_bind" {
  metadata {
    name      = "airflow-operator-bind"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.airflow_operator.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = "airflow-sa"
    namespace = kubernetes_namespace.airflow.metadata[0].name
  }

  depends_on = [
    kubernetes_role.airflow_operator
  ]
}