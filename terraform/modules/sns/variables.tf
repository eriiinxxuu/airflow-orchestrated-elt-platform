variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "alert_email" {
  description = "Email address to subscribe to pipeline failure alerts"
  type        = string
}

variable "mwaa_execution_role_arn" {
  description = "ARN of the MWAA execution role that needs sns:Publish permission"
  type        = string
}
