variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "mwaa_env_arn" {
  description = "ARN of the MWAA environment the Lambda is allowed to generate tokens for"
  type        = string
}
