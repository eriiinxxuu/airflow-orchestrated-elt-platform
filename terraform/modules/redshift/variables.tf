variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the Redshift Serverless security group"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for the Redshift Serverless namespace"
  type        = list(string)
}

variable "vpc_cidr" {
  description = "VPC CIDR block — allowed inbound to Redshift port 5439"
  type        = string
}

variable "admin_username" {
  description = "Redshift Serverless admin username"
  type        = string
  default     = "admin"
}

variable "admin_password" {
  description = "Redshift Serverless admin password"
  type        = string
  sensitive   = true
}

variable "database_name" {
  description = "Name of the initial database"
  type        = string
  default     = "yf_elt"
}

variable "base_capacity" {
  description = "Base RPU capacity (8 to 512 in increments of 8)"
  type        = number
  default     = 8
}

variable "data_bucket_arn" {
  description = "ARN of the S3 data bucket Redshift will COPY from"
  type        = string
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for Redshift event notifications"
  type        = string
}
