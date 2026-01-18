# jenkins-rbac.tf

resource "kubernetes_service_account" "jenkins_agent" {
  metadata {
    name      = "jenkins-agent"
    namespace = "jenkins"
  }
}

resource "kubernetes_cluster_role" "jenkins_agent_role" {
  metadata {
    name = "jenkins-agent-role"
  }

  # Jenkins Kubernetes plugin needs this to create/attach to agent pods
  rule {
    api_groups = [""]
    resources  = [
      "pods",
      "pods/exec",
      "pods/log",
      "secrets",
      "configmaps"
    ]
    verbs = [
      "get", "list", "watch",
      "create", "delete",
      "patch", "update"
    ]
  }

  # Optional (keep if you want Jenkins to scale deployments etc.)
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "deployments/scale"]
    verbs      = ["get", "list", "watch", "patch", "update"]
  }
}

resource "kubernetes_cluster_role_binding" "jenkins_agent_rolebinding" {
  metadata {
    name = "jenkins-agent-rolebinding"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.jenkins_agent_role.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.jenkins_agent.metadata[0].name
    namespace = "jenkins"
  }
}
