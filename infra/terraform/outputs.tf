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