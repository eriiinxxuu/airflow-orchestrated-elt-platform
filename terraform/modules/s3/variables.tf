variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "log_retention_days" {
  description = "Number of days to retain S3 access logs"
  type        = number
  default     = 90
}
