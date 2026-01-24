data "aws_lb_listener" "https" {
  load_balancer_arn = data.aws_lb.eks_alb.arn
  port              = 443
}

resource "aws_lb_target_group" "jenkins" {
  name        = "jenkins-tg"
  port        = 8080
  protocol    = "HTTP"
  vpc_id      = module.networking.vpc_id
  target_type = "ip"
}

resource "aws_lb_target_group_attachment" "jenkins" {
  target_group_arn = aws_lb_target_group.jenkins.arn
  target_id        = module.compute.jenkins_private_ip
  port             = 8080
}

resource "aws_lb_listener_rule" "jenkins" {
  listener_arn = data.aws_lb_listener.https.arn
  priority     = 50

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.jenkins.arn
  }

  condition {
    host_header {
      values = ["jenkins.pushparag.online"]
    }
  }
}
