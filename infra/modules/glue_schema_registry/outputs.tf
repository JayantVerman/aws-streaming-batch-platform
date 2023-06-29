output "registry_name" {
  value = aws_glue_registry.this.registry_name
}

output "registry_arn" {
  value = aws_glue_registry.this.arn
}

output "schema_arns" {
  description = "schema name -> arn"
  value       = { for name, s in aws_glue_schema.schema : s.schema_name => s.arn }
}