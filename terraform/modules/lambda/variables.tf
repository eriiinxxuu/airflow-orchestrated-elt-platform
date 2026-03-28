variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "lambda_role_arn" {
  description = "ARN of the IAM role to attach to the Lambda function"
  type        = string
}

variable "log_group_name" {
  description = "CloudWatch log group name (Lambda depends on this existing first)"
  type        = string
}

variable "airflow_base_url" {
  description = "MWAA webserver URL"
  type        = string
}

variable "airflow_dag_id" {
  description = "Airflow DAG ID to trigger when earnings are detected"
  type        = string
  default     = "yf_event_earnings"
}

variable "watchlist" {
  description = "List of stock ticker symbols to monitor for earnings"
  type        = list(string)
  default     = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "JNJ", "V"]
}

variable "days_ahead" {
  description = "How many days ahead to look for upcoming earnings"
  type        = number
  default     = 1
}

variable "mwaa_env_name" {
  description = "MWAA environment name used to generate the web login token"
  type        = string
}

variable "subnet_ids" {
  description = "List of private subnet IDs for the Lambda VPC config"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for the Lambda VPC config"
  type        = list(string)
}

variable "timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 60
}

variable "memory_size" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 256
}
