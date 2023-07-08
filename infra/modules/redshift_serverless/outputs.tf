output "workgroup_name" {
  value = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "workgroup_arn" {
  value = aws_redshiftserverless_workgroup.this.arn
}

output "namespace_arn" {
  value = aws_redshiftserverless_namespace.this.arn
}

# Workgroup endpoint — the JDBC target for the warehouse load job.
output "endpoint_address" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].address
}

output "endpoint_port" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].port
}