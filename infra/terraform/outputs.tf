output "inference_lambda_arn" {
  description = "ARN de la Lambda de inferencia"
  value       = aws_lambda_function.inference.arn
}

output "inference_lambda_name" {
  description = "Nombre de la Lambda de inferencia"
  value       = aws_lambda_function.inference.function_name
}

output "inference_queue_url" {
  description = "URL de la cola SQS de inferencia (enviar aquí los eventos a predecir)"
  value       = aws_sqs_queue.inference_input.url
}

output "inference_queue_arn" {
  description = "ARN de la cola SQS de inferencia"
  value       = aws_sqs_queue.inference_input.arn
}

output "inference_dlq_url" {
  description = "URL de la Dead Letter Queue de inferencia"
  value       = aws_sqs_queue.inference_dlq.url
}
