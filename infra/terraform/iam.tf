data "aws_caller_identity" "current" {}

# ─── Rol de ejecución para la Lambda de inferencia ───────────────────────────

resource "aws_iam_role" "lambda_inference" {
  name = "${var.lambda_inference_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_inference" {
  name = "${var.lambda_inference_name}-policy"
  role = aws_iam_role.lambda_inference.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      # CloudWatch Logs
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_inference_name}:*"
      },

      # S3: escribir resultados de inferencia
      {
        Sid    = "S3PutInference"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = "arn:aws:s3:::${var.s3_processed_bucket}/inference/*"
      },

      # DynamoDB: escribir predicciones
      {
        Sid    = "DynamoDBWrite"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = "arn:aws:dynamodb:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.dynamodb_table}"
      },

      # SQS: consumir mensajes de la cola de inferencia
      {
        Sid    = "SQSConsumeInference"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = aws_sqs_queue.inference_input.arn
      },

      # SQS: enviar mensajes fallidos a la DLQ (dead_letter_config de Lambda)
      {
        Sid    = "SQSSendDLQ"
        Effect = "Allow"
        Action = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.inference_dlq.arn
      }
    ]
  })
}
