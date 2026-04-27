resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project_name}-raw-${data.aws_caller_identity.current.account_id}"
  force_destroy = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "raw"
  }
}

resource "aws_s3_bucket" "processed" {
  bucket        = "${var.project_name}-processed-${data.aws_caller_identity.current.account_id}"
  force_destroy = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "processed"
  }
}

resource "aws_s3_bucket" "dlq" {
  bucket        = "${var.project_name}-dlq-${data.aws_caller_identity.current.account_id}"
  force_destroy = true

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "dlq"
  }
}

# Bloquear acceso público en todos los buckets
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}