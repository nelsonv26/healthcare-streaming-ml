resource "aws_sqs_queue" "raw" {
  name                      = "${var.project_name}-raw"
  message_retention_seconds = 86400
  visibility_timeout_seconds = 30

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_sqs_queue" "processed" {
  name                      = "${var.project_name}-processed"
  message_retention_seconds = 86400
  visibility_timeout_seconds = 30

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-dlq"
  message_retention_seconds = 1209600

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}