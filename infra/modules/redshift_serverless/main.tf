resource "aws_redshiftserverless_namespace" "this" {
  namespace_name = var.namespace_name
  db_name        = "dev"
  tags           = var.tags
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name = var.workgroup_name
  base_capacity  = var.base_capacity

  # Not VPC-restricted by default (publicly_accessible=false + an
  # aws_redshiftserverless_endpoint_access can be added for a private VPC
  # endpoint — see module README). This keeps the demo plan-safe.
  publicly_accessible = var.publicly_accessible

  # Auto-pause is the main cost control — the warehouse "sleeps" when idle.
  config_parameters {
    parameter_key   = "auto_pause"
    parameter_value = "true"
  }
  config_parameters {
    parameter_key   = "auto_pause_timeout_seconds"
    parameter_value = tostring(var.auto_pause_seconds)
  }

  tags = var.tags
}