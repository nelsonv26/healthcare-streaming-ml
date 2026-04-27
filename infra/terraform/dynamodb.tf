resource "aws_dynamodb_table" "consent_state" {
  name         = "${var.project_name}-consent-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "consent_id"

  attribute {
    name = "consent_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}