resource "aws_secretsmanager_secret" "airflow_api" {
  name                    = "yf-elt/${var.environment}/airflow-api-credentials"
  recovery_window_in_days = var.recovery_window_in_days
}

resource "aws_secretsmanager_secret_version" "airflow_api" {
  secret_id     = aws_secretsmanager_secret.airflow_api.id
  secret_string = jsonencode({ username = "admin", password = "CHANGE_ME" })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
