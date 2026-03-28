output "mwaa_webserver_url" {
  description = "MWAA Airflow webserver URL"
  value       = module.mwaa.webserver_url
}

output "mwaa_environment_name" {
  description = "MWAA environment name"
  value       = module.mwaa.environment_name
}

output "mwaa_execution_role_arn" {
  description = "ARN of the MWAA execution role"
  value       = module.mwaa.execution_role_arn
}

output "mwaa_bucket_name" {
  description = "S3 bucket for DAGs, plugins and requirements — upload your DAGs here"
  value       = module.s3.mwaa_bucket_name
}

output "data_bucket_name" {
  description = "S3 bucket for raw ECS extractor data — set this as Airflow Variable s3_bucket"
  value       = module.s3.data_bucket_name
}

output "sns_topic_arn" {
  description = "SNS alerts topic ARN — set this as Airflow Variable sns_topic_arn"
  value       = module.sns.topic_arn
}

output "vpc_id" {
  description = "VPC ID"
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = module.network.private_subnet_ids
}

output "lambda_function_name" {
  description = "Earnings trigger Lambda function name"
  value       = module.lambda.function_name
}

output "eventbridge_rule_name" {
  description = "EventBridge rule name"
  value       = module.eventbridge.rule_name
}

output "log_group_name" {
  description = "CloudWatch log group for Lambda logs"
  value       = module.cloudwatch.log_group_name
}

output "ecs_cluster_name" {
  description = "ECS cluster name — set this as Airflow Variable ecs_config.cluster_arn"
  value       = module.ecs.cluster_name
}

output "ecr_repository_url" {
  description = "ECR repository URL — push your extractor image here"
  value       = module.ecs.ecr_repository_url
}

output "ecs_task_definition_arn" {
  description = "ECS task definition ARN — set this as Airflow Variable ecs_config.task_definition"
  value       = module.ecs.task_definition_arn
}

output "ecs_task_security_group_id" {
  description = "ECS task security group ID — set this as Airflow Variable ecs_config.security_group"
  value       = module.ecs.task_security_group_id
}

output "redshift_endpoint" {
  description = "Redshift Serverless endpoint — use as host in Airflow connection redshift_default"
  value       = module.redshift.workgroup_endpoint
  sensitive   = true
}

output "redshift_database" {
  description = "Redshift database name"
  value       = module.redshift.database_name
}

output "redshift_s3_iam_role_arn" {
  description = "IAM role ARN used in Redshift COPY statements"
  value       = module.redshift.s3_iam_role_arn
}
