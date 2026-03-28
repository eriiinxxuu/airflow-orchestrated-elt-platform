# ── SNS Topic ─────────────────────────────────────────────────
resource "aws_sns_topic" "airflow_alerts" {
  name = "yf-elt-airflow-alerts-${var.environment}"
}

# ── Email subscription ────────────────────────────────────────
# AWS will send a confirmation email to this address.
# The subscription is only active once the recipient clicks confirm.
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.airflow_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── Topic policy ──────────────────────────────────────────────
# Allow the MWAA execution role to publish messages.
# dag_utils.py calls boto3 sns.publish() using this role.
resource "aws_sns_topic_policy" "airflow_publish" {
  arn = aws_sns_topic.airflow_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowMWAAPublish"
        Effect = "Allow"
        Principal = {
          AWS = var.mwaa_execution_role_arn
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.airflow_alerts.arn
      },
      {
        Sid    = "AllowRedshiftPublish"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
        Action   = "sns:Publish"
        Resource = aws_sns_topic.airflow_alerts.arn
      },
    ]
  })
}
