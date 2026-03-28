output "log_group_name" {
  description = "Name of the CloudWatch log group for the Lambda function"
  value       = aws_cloudwatch_log_group.lambda.name
}

output "log_group_arn" {
  description = "ARN of the CloudWatch log group"
  value       = aws_cloudwatch_log_group.lambda.arn
}

output "alarm_mwaa_unhealthy_arn" {
  description = "ARN of the MWAA unhealthy environment alarm"
  value       = aws_cloudwatch_metric_alarm.mwaa_unhealthy.arn
}

output "alarm_ecs_task_failures_arn" {
  description = "ARN of the ECS task failures alarm"
  value       = aws_cloudwatch_metric_alarm.ecs_task_failures.arn
}

output "alarm_lambda_errors_arn" {
  description = "ARN of the Lambda errors alarm"
  value       = aws_cloudwatch_metric_alarm.lambda_errors.arn
}
