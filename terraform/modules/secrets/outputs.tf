output "secret_arn" {
  description = "ARN of the Airflow API credentials secret"
  value       = aws_secretsmanager_secret.airflow_api.arn
}

output "secret_name" {
  description = "Name of the Airflow API credentials secret"
  value       = aws_secretsmanager_secret.airflow_api.name
}
