output "topic_arn" {
  description = "ARN of the SNS alerts topic"
  value       = aws_sns_topic.airflow_alerts.arn
}

output "topic_name" {
  description = "Name of the SNS alerts topic"
  value       = aws_sns_topic.airflow_alerts.name
}
