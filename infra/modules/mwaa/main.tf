resource "aws_mwaa_environment" "this" {
  name                 = "${var.project}-mwaa"
  execution_role_arn   = var.execution_role_arn
  source_bucket_arn    = var.source_bucket_arn
  dag_s3_path          = var.dag_s3_path
  airflow_version      = var.airflow_version
  environment_class    = var.environment_class
  webserver_access_mode = var.webserver_access_mode
  schedulers           = var.schedulers
  min_workers          = var.min_workers
  max_workers          = var.max_workers

  requirements_s3_path = var.requirements_s3_key != "" ? var.requirements_s3_key : null

  network_configuration {
    security_group_ids = var.security_group_ids
    subnet_ids         = var.subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled = true
      log_level = "INFO"
    }
    task_logs {
      enabled = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled = false
    }
    worker_logs {
      enabled = true
      log_level = "INFO"
    }
  }

  tags = var.tags
}