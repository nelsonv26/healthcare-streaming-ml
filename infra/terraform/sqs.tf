# ─── Cola de entrada para la Lambda de inferencia ────────────────────────────

resource "aws_sqs_queue" "inference_input" {
  name                       = var.inference_queue_name
  visibility_timeout_seconds = 60       # debe ser >= Lambda timeout (30 s)
  message_retention_seconds  = 86400    # 1 día
  receive_wait_time_seconds  = 20       # long polling

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.inference_dlq.arn
    maxReceiveCount     = 3
  })
}

# ─── Dead Letter Queue ────────────────────────────────────────────────────────

resource "aws_sqs_queue" "inference_dlq" {
  name                      = "${var.inference_queue_name}-dlq"
  message_retention_seconds = 259200   # 3 días para investigar fallos
}

# Política que permite a la Lambda de inferencia usar la DLQ como destino
resource "aws_sqs_queue_policy" "inference_input" {
  queue_url = aws_sqs_queue.inference_input.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.inference_input.arn
    }]
  })
}
