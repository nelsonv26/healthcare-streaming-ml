variable "aws_region" {
  description = "AWS region donde se despliegan los recursos"
  type        = string
  default     = "us-east-1"
}

variable "s3_processed_bucket" {
  description = "Bucket S3 donde se guardan los resultados de inferencia"
  type        = string
  default     = "healthcare-streaming-processed-905921697186"
}

variable "dynamodb_table" {
  description = "Nombre de la tabla DynamoDB para resultados de inferencia"
  type        = string
  default     = "consent-state-aws"
}

variable "model_key" {
  description = "Clave S3 del modelo pkl (reservado para reintegración futura de sklearn)"
  type        = string
  default     = "models/risk_model.pkl"
}

variable "lambda_inference_name" {
  description = "Nombre de la función Lambda de inferencia"
  type        = string
  default     = "healthcare-inference"
}

variable "lambda_processor_name" {
  description = "Nombre de la función Lambda del processor (ya desplegada)"
  type        = string
  default     = "healthcare-processor"
}

variable "inference_queue_name" {
  description = "Nombre de la cola SQS que alimenta la Lambda de inferencia"
  type        = string
  default     = "healthcare-inference-queue"
}
