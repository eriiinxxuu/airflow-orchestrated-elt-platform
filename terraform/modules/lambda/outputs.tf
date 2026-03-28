output "function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.earnings_trigger.arn
}

output "function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.earnings_trigger.function_name
}
