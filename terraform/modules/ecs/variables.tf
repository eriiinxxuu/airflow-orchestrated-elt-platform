variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the ECS task security group"
  type        = string
}

variable "data_bucket_arn" {
  description = "ARN of the S3 data bucket ECS tasks write to"
  type        = string
}

variable "data_bucket_name" {
  description = "Name of the S3 data bucket (injected as env var into the container)"
  type        = string
}

variable "task_cpu" {
  description = "CPU units for the ECS task (1 vCPU = 1024)"
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Memory in MB for the ECS task"
  type        = number
  default     = 1024
}
