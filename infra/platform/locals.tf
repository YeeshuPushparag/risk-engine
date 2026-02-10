locals {
  project     = "risk"
  environment = "platform"
  name_prefix = "${local.project}-${local.environment}"
}
locals {
  acm_certificate_arn = module.compute.acm_certificate_arn
  jenkins_instance_id = module.compute.jenkins_instance_id
}
