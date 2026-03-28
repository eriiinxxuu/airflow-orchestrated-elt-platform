variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "lambda_arn" {
  description = "ARN of the Lambda function to invoke"
  type        = string
}

variable "lambda_function_name" {
  description = "Name of the Lambda function (required for aws_lambda_permission)"
  type        = string
}

variable "schedule_expression" {
  description = "EventBridge cron or rate expression"
  type        = string
  default     = "cron(0 6 ? * MON-FRI *)"
}
