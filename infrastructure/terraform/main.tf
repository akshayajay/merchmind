data "aws_caller_identity" "current" {}

locals {
  bucket_name = "${var.project_name}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  prefixes    = ["bronze/", "silver/", "gold/", "reports/", "athena-results/"]
}

resource "aws_s3_bucket" "lake" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "archive-bronze"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_object" "prefixes" {
  for_each = toset(local.prefixes)
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""
}

resource "aws_s3_bucket_policy" "secure_transport" {
  bucket = aws_s3_bucket.lake.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_glue_catalog_database" "analytics" {
  name = replace(var.project_name, "-", "_")
}

resource "aws_athena_workgroup" "analytics" {
  name  = var.project_name
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 10737418240
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_cloudwatch_log_group" "spark" {
  name              = "/aws/merchmind/${var.project_name}/spark"
  retention_in_days = var.log_retention_days
}

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark"
  release_label = "emr-7.5.0"
  type          = "SPARK"

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

resource "aws_iam_role" "emr_execution" {
  name = "${var.project_name}-emr-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "emr_execution" {
  name = "${var.project_name}-data-and-logs"
  role = aws_iam_role.emr_execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.lake.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.lake.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"]
        Resource = ["*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.spark.arn}:*"]
      }
    ]
  })
}

resource "aws_security_group" "msk" {
  count       = var.enable_msk_serverless ? 1 : 0
  name        = "${var.project_name}-msk"
  description = "MSK Serverless client traffic"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    precondition {
      condition     = var.vpc_id != "" && length(var.private_subnet_ids) >= 2
      error_message = "Set vpc_id and at least two private_subnet_ids when enabling MSK."
    }
  }
}

resource "aws_msk_serverless_cluster" "events" {
  count        = var.enable_msk_serverless ? 1 : 0
  cluster_name = "${var.project_name}-events"

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.msk[0].id]
  }

  client_authentication {
    sasl { iam { enabled = true } }
  }
}
