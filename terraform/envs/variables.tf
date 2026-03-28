variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-southeast-2"
}

# ── Network ───────────────────────────────────────────────────
variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

# ── Redshift ──────────────────────────────────────────────────
variable "redshift_admin_username" {
  description = "Redshift Serverless admin username"
  type        = string
  default     = "admin"
}

variable "redshift_admin_password" {
  description = "Redshift Serverless admin password"
  type        = string
  sensitive   = true
}

# ── MWAA ──────────────────────────────────────────────────────
variable "mwaa_environment_class" {
  description = "MWAA environment class (mw1.small / mw1.medium / mw1.large)"
  type        = string
  default     = "mw1.small"
}

variable "mwaa_min_workers" {
  description = "Minimum number of MWAA workers"
  type        = number
  default     = 1
}

variable "mwaa_max_workers" {
  description = "Maximum number of MWAA workers"
  type        = number
  default     = 5
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the MWAA webserver (office/VPN IP)"
  type        = list(string)
  default     = []
}

# ── Lambda / EventBridge ──────────────────────────────────────
variable "watchlist" {
  description = "Stock ticker symbols to monitor for earnings"
  type        = list(string)
  default     = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "JNJ", "V"]
}

variable "days_ahead" {
  description = "How many days ahead to look for upcoming earnings"
  type        = number
  default     = 1
}

# ── Alerting ──────────────────────────────────────────────────
variable "alert_email" {
  description = "Email address to receive pipeline failure alerts via SNS"
  type        = string
}

variable "github_org" {
  description = "GitHub username or organisation"
  type        = string
}

variable "github_repo" {
  description = "GitHub repository name"
  type        = string
}