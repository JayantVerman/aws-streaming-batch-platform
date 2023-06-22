locals {
  prefix = var.project
  s3_arns = concat(var.s3_bucket_arns, var.dags_bucket_arn != "" ? [var.dags_bucket_arn] : [])
}

# ---------------------------------------------------------------- EMR (EC2)
# Instance profile attached to each EC2 node: read/write the lake + stream logs.
resource "aws_iam_role" "emr_ec2" {
  name = "${local.prefix}-emr-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_instance_profile" "emr_ec2" {
  name = "${local.prefix}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2.name
}

resource "aws_iam_policy" "emr_ec2" {
  name        = "${local.prefix}-emr-ec2"
  description = "Lake read/write + CloudWatch + Glue for EMR EC2 nodes"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = concat(
          [for a in local.s3_arns : "${a}", "${a}/*"]
        )
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      },
    ], var.glue_registry_arn != "" ? [
      {
        Effect = "Allow"
        Action = ["glue:GetSchemaVersion", "glue:GetSchema", "glue:ListSchemas"]
        Resource = [var.glue_registry_arn, "${var.glue_registry_arn}/*"]
      }
    ] : [])
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2" {
  role       = aws_iam_role.emr_ec2.name
  policy_arn = aws_iam_policy.emr_ec2.arn
}

# EMR service role (assumed by Elastic MapReduce).
resource "aws_iam_role" "emr_service" {
  name = "${local.prefix}-emr-service"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "emr_service" {
  name        = "${local.prefix}-emr-service"
  description = "EMR service: manage cluster EC2 + lake access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["ec2:Describe*", "ec2:RunInstances", "ec2:TerminateInstances",
                  "ec2:CreateTags", "iam:PassRole"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:log-group:/aws/emr/*:*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service.name
  policy_arn = aws_iam_policy.emr_service.arn
}

# ---------------------------------------------------------------- MWAA
resource "aws_iam_role" "mwaa" {
  name = "${local.prefix}-mwaa"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = "sts:AssumeRole"
      Principal = { Service = "airflow.amazonaws.com" }
    }]
  })
}

resource "aws_iam_policy" "mwaa" {
  name        = "${local.prefix}-mwaa"
  description = "MWAA execution: read dags bundle S3 + CloudWatch logs"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:GetBucketLocation", "s3:ListBucket",
                  "s3:PutObject"]
        Resource = concat(
          [for a in local.s3_arns : "${a}", "${a}/*"]
        )
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogStream", "logs:PutLogEvents",
                  "logs:CreateLogGroup", "logs:DescribeLogGroups",
                  "logs:DescribeLogStreams"]
        Resource = "arn:aws:logs:*:*:log-group:/aws/mwaa/${local.prefix}*:*"
      },
      {
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:DescribeKey"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "mwaa" {
  role       = aws_iam_role.mwaa.name
  policy_arn = aws_iam_policy.mwaa.arn
}