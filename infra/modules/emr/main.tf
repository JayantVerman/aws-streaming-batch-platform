resource "aws_emr_cluster" "this" {
  name                      = "${var.project}-emr"
  release_label             = var.release_label
  applications              = ["Hadoop", "Spark"]
  service_role              = var.service_role_arn
  termination_protection    = false
  keep_job_flow_alive_when_no_steps = var.keep_alive

  # Subnet + instance profile only; AWS auto-manages the EMR security groups,
  # which keeps this module plan-safe and avoids hand-authored SG rules.
  ec2_attributes {
    subnet_id                         = var.subnet_id
    instance_profile                  = var.instance_profile
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
    ebs_config {
      size                 = var.ebs_size_gb
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    market         = var.enable_spot ? "SPOT" : "ON_DEMAND"
    ebs_config {
      size                 = var.ebs_size_gb
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  tags = var.tags
}