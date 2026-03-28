variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "log_retention_days" {
  description = "Number of days to retain Lambda logs in CloudWatch"
  type        = number
  default     = 30
}

variable "sns_topic_arn" {
  description = "SNS topic ARN to send CloudWatch alarm notifications to"
  type        = string
}

variable "mwaa_env_name" {
  description = "MWAA environment name (used to scope MWAA alarms)"
  type        = string
}

variable "ecs_cluster_name" {
  description = "ECS cluster name (used to scope ECS alarms)"
  type        = string
}

variable "lambda_function_name" {
  description = "Earnings trigger Lambda function name (used to scope Lambda alarms)"
  type        = string
}
