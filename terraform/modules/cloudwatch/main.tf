resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/yf-earnings-trigger-${var.environment}"
  retention_in_days = var.log_retention_days
}

# ── MWAA Alarms ───────────────────────────────────────────────

# Fires when the MWAA environment reports an unhealthy state.
# This catches infrastructure-level failures that Airflow callbacks cannot.
resource "aws_cloudwatch_metric_alarm" "mwaa_unhealthy" {
  alarm_name          = "yf-elt-mwaa-unhealthy-${var.environment}"
  alarm_description   = "MWAA environment is reporting an unhealthy status"
  namespace           = "AmazonMWAA"
  metric_name         = "EnvironmentHealth"
  dimensions          = { Name = var.mwaa_env_name }
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 2
  threshold           = 10  # 10 = unhealthy in MWAA health metric scale
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [var.sns_topic_arn]
  ok_actions          = [var.sns_topic_arn]
}

# Fires when queued tasks have been waiting too long — likely a worker shortage.
resource "aws_cloudwatch_metric_alarm" "mwaa_queue_backlog" {
  alarm_name          = "yf-elt-mwaa-queue-backlog-${var.environment}"
  alarm_description   = "MWAA has too many queued tasks — possible worker shortage"
  namespace           = "AmazonMWAA"
  metric_name         = "QueuedTasks"
  dimensions          = { Name = var.mwaa_env_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 20
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_topic_arn]
}

# ── ECS Alarms ────────────────────────────────────────────────

# Fires when ECS tasks are failing — covers extractor container crashes.
resource "aws_cloudwatch_metric_alarm" "ecs_task_failures" {
  alarm_name          = "yf-elt-ecs-task-failures-${var.environment}"
  alarm_description   = "ECS tasks are failing — extractor container may be crashing"
  namespace           = "ECS/ContainerInsights"
  metric_name         = "TaskCount"
  dimensions = {
    ClusterName = var.ecs_cluster_name
    LaunchType  = "FARGATE"
  }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 5
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_topic_arn]
}

# ── Lambda Alarms ─────────────────────────────────────────────

# Fires when the earnings trigger Lambda is throwing errors.
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "yf-elt-lambda-errors-${var.environment}"
  alarm_description   = "Earnings trigger Lambda is throwing errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = var.lambda_function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_topic_arn]
}

# Fires when the Lambda is being throttled — too many concurrent invocations.
resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  alarm_name          = "yf-elt-lambda-throttles-${var.environment}"
  alarm_description   = "Earnings trigger Lambda is being throttled"
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  dimensions          = { FunctionName = var.lambda_function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.sns_topic_arn]
}
