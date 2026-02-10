module "networking" {
  source = "./networking"

  project     = local.project
  environment = local.environment
  name_prefix = local.name_prefix
}

module "compute" {
  source = "./compute"

  jenkins_key_name = "risk-jenkins-key"
  name_prefix      = local.name_prefix

  vpc_id         = module.networking.vpc_id
  public_subnets = module.networking.public_subnet_ids
}

module "storage" {
  source = "./storage"
}
