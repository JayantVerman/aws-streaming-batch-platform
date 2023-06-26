resource "aws_glue_registry" "this" {
  registry_name = var.registry_name
  description   = "Avro schemas for the retail Kinesis streams"
}

resource "aws_glue_schema" "schema" {
  for_each          = var.schemas
  schema_name       = each.key
  compatibility     = var.compatibility
  data_format       = "AVRO"
  registry_arn      = aws_glue_registry.this.arn
  schema_definition = each.value
}

# Pin the published schema to its first version (schemas above embed the
# definition; this just makes the version explicit and stable).
resource "aws_glue_schema_version" "version" {
  for_each = var.schemas
  schema_id {
    schema_arn = aws_glue_schema.schema[each.key].arn
  }
  schema_definition = each.value
}