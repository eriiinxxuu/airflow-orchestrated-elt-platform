variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name for DAGs, plugins and requirements"
  type        = string
}

variable "s3_bucket_arn" {
  description = "S3 bucket ARN for DAGs, plugins and requirements"
  type        = string
}

variable "data_bucket_arn" {
  description = "S3 data bucket ARN (ECS writes here, MWAA reads for Redshift COPY)"
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for the MWAA environment (minimum 2 AZs)"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID for the MWAA security group"
  type        = string
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to reach the MWAA webserver (use your office/VPN IP)"
  type        = list(string)
  default     = []
}

variable "airflow_version" {
  description = "Airflow version to use"
  type        = string
  default     = "2.9.2"
}

variable "environment_class" {
  description = "MWAA environment class (mw1.small / mw1.medium / mw1.large)"
  type        = string
  default     = "mw1.small"
}

variable "min_workers" {
  description = "Minimum number of workers"
  type        = number
  default     = 1
}

variable "max_workers" {
  description = "Maximum number of workers"
  type        = number
  default     = 5
}

variable "schedulers" {
  description = "Number of schedulers"
  type        = number
  default     = 2
}

variable "webserver_access_mode" {
  description = "PUBLIC_ONLY or PRIVATE_ONLY"
  type        = string
  default     = "PRIVATE_ONLY"
}

variable "dag_s3_path" {
  description = "S3 path for DAGs folder"
  type        = string
  default     = "dags/"
}

variable "plugins_s3_path" {
  description = "S3 path for the plugins zip file"
  type        = string
  default     = "plugins/plugins.zip"
}

variable "requirements_s3_path" {
  description = "S3 path for the requirements.txt file"
  type        = string
  default     = "requirements/requirements.txt"
}

variable "sns_topic_arn" {
  description = "SNS topic ARN for pipeline failure alerts (stored as Airflow Variable)"
  type        = string
}

variable "plugins_s3_object_version" {
  description = "Version ID of the plugins.zip S3 object — used to enforce depends_on ordering"
  type        = string
}

variable "requirements_s3_object_version" {
  description = "Version ID of the requirements.txt S3 object — used to enforce depends_on ordering"
  type        = string
}

variable "airflow_configuration_options" {
  description = "Map of Airflow configuration overrides"
  type        = map(string)
  default     = {}
}
