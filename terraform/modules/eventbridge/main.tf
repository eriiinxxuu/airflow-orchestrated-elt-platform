resource "aws_cloudwatch_event_rule" "daily_check" {
  name                = "yf-earnings-check-${var.environment}"
  description         = "Triggers earnings Lambda daily at 06:00 UTC on weekdays"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.daily_check.name
  target_id = "EarningsLambda"
  arn       = var.lambda_arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_check.arn
}
