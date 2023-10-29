# LocalStack (tflocal) parity root — same modules as envs/aws, no AWS account.

locals {
  bucket_names = ["retail-local-lake", "retail-local-checkpoints", "retail-local-dags"]

  schemas = {
    orders      = file("${path.module}/../../../streaming/schemas/orders.avsc")
    order_items = file("${path.module}/../../../streaming/schemas/order_items.avsc")
  }
}

module "streams" {
  source      = "../../modules/kinesis"
  streams     = ["retail-orders", "retail-order_items"]
  shard_count = 1
}

module "lake" {
  source        = "../../modules/s3"
  bucket_names  = local.bucket_names
}

module "schemas" {
  source        = "../../modules/glue_schema_registry"
  registry_name = "retail-schemas"
  schemas       = local.schemas
}

module "iam" {
  source            = "../../modules/iam"
  project           = "retail"
  s3_bucket_arns    = [for b in module.lake.bucket_arns : b]
  glue_registry_arn = module.schemas.registry_arn
}
# wip164

/* wip */
