output "environment_id" {
  value = aws_mwaa_environment.this.id
}

output "environment_arn" {
  value = aws_mwaa_environment.this.arn
}

output "webserver_url" {
  value = aws_mwaa_environment.this.webserver_url
}

output "airflow_version" {
  value = aws_mwaa_environment.this.airflow_version
}