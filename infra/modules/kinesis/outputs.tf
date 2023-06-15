output "stream_names" {
  description = "Names of the created streams"
  value       = toset(keys(aws_kinesis_stream.stream))
}

output "stream_arns" {
  description = "name -> ARN map"
  value       = { for name, s in aws_kinesis_stream.stream : s.name => s.arn }
}