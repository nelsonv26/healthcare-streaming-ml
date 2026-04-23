data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../../lambda/lambda_function.py"
  output_path = "${path.module}/lambda_function.zip"
}

resource "aws_lambda_function" "processor" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-processor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 256
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      S3_RAW_BUCKET       = aws_s3_bucket.raw.bucket
      S3_PROCESSED_BUCKET = aws_s3_bucket.processed.bucket
      DYNAMODB_TABLE_AWS  = aws_dynamodb_table.consent_state.name
      SQS_PROCESSED_URL   = aws_sqs_queue.processed.url
      SQS_DLQ_URL         = aws_sqs_queue.dlq.url
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.raw.arn
  function_name    = aws_lambda_function.processor.arn
  batch_size       = 10
  enabled          = true
}

resource "aws_lambda_layer_version" "sklearn" {
  layer_name          = "${var.project_name}-sklearn"
  compatible_runtimes = ["python3.11"]

  s3_bucket = aws_s3_bucket.processed.bucket
  s3_key    = "layers/sklearn_layer.zip"
}

data "archive_file" "inference_zip" {
  type        = "zip"
  source_file = "${path.module}/../../lambda/lambda_inference.py"
  output_path = "${path.module}/lambda_inference.zip"
}

resource "aws_lambda_function" "inference" {
  filename         = data.archive_file.inference_zip.output_path
  function_name    = "${var.project_name}-inference"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_inference.lambda_handler"
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 512
  source_code_hash = data.archive_file.inference_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.sklearn.arn]

  environment {
    variables = {
      S3_PROCESSED_BUCKET = aws_s3_bucket.processed.bucket
      DYNAMODB_TABLE_AWS  = aws_dynamodb_table.consent_state.name
      MODEL_KEY           = "models/risk_model.pkl"
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}