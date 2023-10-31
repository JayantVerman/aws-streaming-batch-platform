# Real-AWS deployment root. Wire the seven modules together.

locals {
  bucket_suffix = random_id.bucket_suffix.hex

  # One stream per streaming entity (matches stream_name_template in
  # config/settings.yaml: retail-{entity}).
  stream_names = ["retail-orders", "retail-order_items"]

  bucket_names = [
    "${var.project}-${local.bucket_suffix}-lake",
    "${var.project}-${local.bucket_suffix}-checkpoints",
    "${var.project}-${local.bucket_suffix}-dags",
  ]

  # Avro schemas are the single source of truth in streaming/schemas/ — the
  # registry module is fed the same files, never a copy.
  schemas = {
    orders      = file("${path.module}/../../../streaming/schemas/orders.avsc")
    order_items = file("${path.module}/../../../streaming/schemas/order_items.avsc")
  }

  lake_bucket_arn        = module.lake.bucket_arns["${local.bucket_names[0]}"]
  checkpoints_bucket_arn = module.lake.bucket_arns["${local.bucket_names[1]}"]
  dags_bucket_arn        = module.lake.bucket_arns["${local.bucket_names[2]}"]
}

resource "random_id" "bucket_suffix" {
  byte_length = 4 # s3 bucket names are globally unique
}

# ------------------------------------------------------------ core (always on)
module "streams" {
  source   = "../../modules/kinesis"
  streams  = local.stream_names
  shard_count = 1
  tags     = { Name = "retail-streams" }
}

module "lake" {
  source   = "../../modules/s3"
  bucket_names = local.bucket_names
  tags = { Name = "retail-lake" }
}

module "schemas" {
  source        = "../../modules/glue_schema_registry"
  registry_name = "retail-schemas"
  schemas       = local.schemas
}

module "iam" {
  source            = "../../modules/iam"
  project           = var.project
  s3_bucket_arns    = values(module.lake.bucket_arns)
  dags_bucket_arn   = local.dags_bucket_arn
  glue_registry_arn = module.schemas.registry_arn
}

# --------------------------------------------------------------- gated heavy
module "emr" {
  source            = "../../modules/emr"
  count             = var.deploy_emr ? 1 : 0
  project           = var.project
  subnet_id         = var.emr_subnet_id
  instance_profile  = module.iam.emr_ec2_instance_profile
  service_role_arn  = module.iam.emr_service_role_arn
  tags              = { Name = "retail-emr" }
}

module "mwaa" {
  source                = "../../modules/mwaa"
  count                 = var.deploy_mwaa ? 1 : 0
  project               = var.project
  execution_role_arn    = module.iam.mwaa_execution_role_arn
  source_bucket_arn     = local.dags_bucket_arn
  dag_s3_path           = "dags/"
  requirements_s3_key   = "requirements/requirements.txt"
  subnet_ids            = var.mwaa_subnet_ids
  security_group_ids    = var.mwaa_security_group_ids
  tags                  = { Name = "retail-mwaa" }
}

module "redshift" {
  source            = "../../modules/redshift_serverless"
  count             = var.deploy_redshift ? 1 : 0
  project           = var.project
  publicly_accessible = false
  tags              = { Name = "retail-redshift" }
}
