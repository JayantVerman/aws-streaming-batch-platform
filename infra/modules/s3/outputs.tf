output "bucket_ids" {
  description = "Bucket name -> id"
  value       = { for name, b in aws_s3_bucket.bucket : b.id => b.id }
}

output "bucket_arns" {
  description = "Bucket name -> arn"
  value       = { for name, b in aws_s3_bucket.bucket : b.id => b.arn }
}