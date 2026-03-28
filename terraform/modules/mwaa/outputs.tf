output "environment_name" {
  description = "Name of the MWAA environment"
  value       = aws_mwaa_environment.main.name
}

output "environment_arn" {
  description = "ARN of the MWAA environment"
  value       = aws_mwaa_environment.main.arn
}

output "webserver_url" {
  description = "MWAA webserver URL (use this as airflow_base_url)"
  value       = "https://${aws_mwaa_environment.main.webserver_url}"
}

output "execution_role_arn" {
  description = "ARN of the MWAA execution role"
  value       = aws_iam_role.mwaa_execution.arn
}

output "execution_role_name" {
  description = "Name of the MWAA execution role"
  value       = aws_iam_role.mwaa_execution.name
}

output "security_group_id" {
  description = "Security group ID attached to the MWAA environment"
  value       = aws_security_group.mwaa.id
}
