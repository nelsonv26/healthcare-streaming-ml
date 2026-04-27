terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }

  # Para usar state remoto en S3 (recomendado en equipo), descomenta:
  # backend "s3" {
  #   bucket = "healthcare-streaming-processed-905921697186"
  #   key    = "terraform/state/healthcare-inference.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}
