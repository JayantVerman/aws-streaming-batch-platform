output "emr_ec2_instance_profile" {
  description = "Name of the EMR EC2 instance profile"
  value       = aws_iam_instance_profile.emr_ec2.name
}

output "emr_service_role_arn" {
  description = "ARN of the EMR service role"
  value       = aws_iam_role.emr_service.arn
}

output "mwaa_execution_role_arn" {
  description = "ARN of the MWAA execution role"
  value       = aws_iam_role.mwaa.arn
}