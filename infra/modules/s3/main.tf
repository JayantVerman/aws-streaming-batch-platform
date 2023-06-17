resource "aws_s3_bucket" "bucket" {
  for_each = toset(var.bucket_names)
  bucket   = each.value
  force_destroy = var.force_destroy
  tags = merge(var.tags, { name = each.value })
}

resource "aws_s3_bucket_versioning" "versioning" {
  for_each = { b in toset(var.bucket_names) : b => b if var.enable_versioning }
  bucket   = aws_s3_bucket.bucket[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  for_each = { b in toset(var.bucket_names) : b => b }
  bucket   = aws_s3_bucket.bucket[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" // SSE-S3: free, sufficient for demo data
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lifecycle" {
  for_each = var.lifecycle_expire_noncurrent_days > 0 ? { for b in toset(var.bucket_names) : b => b } : {}
  bucket   = aws_s3_bucket.bucket[each.key].id
  rule {
    id     = "expire-noncurrent"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = var.lifecycle_expire_noncurrent_days
    }
  }
}