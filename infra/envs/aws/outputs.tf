output "kinesis_stream_arns" {
  description = "name -> ARN for the retail streams"
  value       = module.streams.stream_arns
}

output "s3_bucket_arns" {
  description = "name -> ARN of the data lake buckets"
  value       = module.lake.bucket_arns
}

output "schema_registry_name" {
  description = "Glue Schema Registry name (set SCHEMA_REGISTRY_NAME on ENV=aws)"
  value       = module.schemas.registry_name
}

output "emr_cluster_id" {
  description = "EMR cluster id (when deploy_emr=true)"
  value       = var.deploy_emr ? module.emr[0].cluster_id : null
}

output "mwaa_webserver_url" {
  description = "MWAA Airflow UI (when deploy_mwaa=true)"
  value       = var.deploy_mwaa ? module.mwaa[0].webserver_url : null
}

output "redshift_endpoint" {
  description = "Redshift Serverless JDBC endpoint (when deploy_redshift=true)"
  value       = var.deploy_redshift ? "${module.redshift[0].endpoint_address}:${module.redshift[0].endpoint_port}" : null
}