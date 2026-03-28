data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/earnings_trigger.py"
  output_path = "${path.module}/earnings_trigger.zip"
}

resource "aws_lambda_function" "earnings_trigger" {
  function_name    = "yf-earnings-trigger-${var.environment}"
  description      = "Checks Yahoo Finance for earnings; triggers Airflow DAG if found"
  role             = var.lambda_role_arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  handler          = "earnings_trigger.handler"
  runtime          = "python3.11"
  timeout          = var.timeout
  memory_size      = var.memory_size

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  environment {
    variables = {
      AIRFLOW_BASE_URL = var.airflow_base_url
      AIRFLOW_DAG_ID   = var.airflow_dag_id
      WATCHLIST        = jsonencode(var.watchlist)
      DAYS_AHEAD       = tostring(var.days_ahead)
      MWAA_ENV_NAME    = var.mwaa_env_name
    }
  }

  depends_on = [var.log_group_name]
}
