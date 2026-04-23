output "sqs_raw_url" {
  value = aws_sqs_queue.raw.url
}

output "sqs_processed_url" {
  value = aws_sqs_queue.processed.url
}

output "sqs_dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "s3_raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "s3_processed_bucket" {
  value = aws_s3_bucket.processed.bucket
}

output "dynamodb_table" {
  value = aws_dynamodb_table.consent_state.name
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda_role.arn
}

output "lambda_function_name" {
  value = aws_lambda_function.processor.function_name
}

## output "glue_job_name" {
##  value = aws_glue_job.raw_to_processed.name
##}

output "lambda_inference_name" {
  value = aws_lambda_function.inference.function_name
}