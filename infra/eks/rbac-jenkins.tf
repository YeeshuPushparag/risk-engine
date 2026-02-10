resource "kubernetes_service_account_v1" "jenkins_agent" {
  metadata {
    name      = "jenkins-agent"
    namespace = kubernetes_namespace.jenkins.metadata[0].name

    annotations = {
      "eks.amazonaws.com/role-arn" = aws_iam_role.jenkins_irsa.arn
    }
  }
}

resource "kubernetes_cluster_role" "jenkins_agent_role" {
  metadata {
    name = "jenkins-agent-role"
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "pods/exec", "pods/log", "secrets", "configmaps"]
    verbs      = ["get", "list", "watch", "create", "delete", "patch", "update"]
  }

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
    name      = kubernetes_service_account_v1.jenkins_agent.metadata[0].name
    namespace = kubernetes_namespace.jenkins.metadata[0].name
  }
}
