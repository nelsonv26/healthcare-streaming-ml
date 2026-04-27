# ─── Empaquetado automático de la Lambda de inferencia ───────────────────────
# archive_file crea el zip en cada terraform apply si el código cambió.
# No es necesario ningún paso manual de build ni Lambda Layer.

data "archive_file" "inference" {
  type        = "zip"
  source_file = "${path.module}/../../lambda/lambda_inference.py"
  output_path = "${path.module}/../../lambda/lambda_inference.zip"
}


# ─── Lambda de inferencia (nueva) ────────────────────────────────────────────

resource "aws_lambda_function" "inference" {
  function_name    = var.lambda_inference_name
  filename         = data.archive_file.inference.output_path
  source_code_hash = data.archive_file.inference.output_base64sha256
  role             = aws_iam_role.lambda_inference.arn
  handler          = "lambda_inference.handler"
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      S3_PROCESSED_BUCKET = var.s3_processed_bucket
      DYNAMODB_TABLE_AWS  = var.dynamodb_table
      MODEL_KEY           = var.model_key  # reservado para futura reintegración de pkl
      LOG_LEVEL           = "INFO"
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.inference_dlq.arn
  }

  depends_on = [aws_iam_role_policy.lambda_inference]
}

# Disparo: SQS → Lambda de inferencia
resource "aws_lambda_event_source_mapping" "inference_sqs" {
  event_source_arn                   = aws_sqs_queue.inference_input.arn
  function_name                      = aws_lambda_function.inference.arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  function_response_types            = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = 5
  }
}


# ─── Lambda del processor (ya desplegada — solo se referencia) ───────────────
# Se usa data source para importar su estado sin recrearla.
# Si la Lambda del processor aún no existe, reemplaza "data" por "resource"
# y apunta su filename al zip del processor adaptado para SQS.

data "aws_lambda_function" "processor" {
  function_name = var.lambda_processor_name
}
