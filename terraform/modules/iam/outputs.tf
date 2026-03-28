output "lambda_role_arn" {
  description = "ARN of the IAM role for the Lambda function"
  value       = aws_iam_role.earnings_lambda.arn
}

output "lambda_role_name" {
  description = "Name of the IAM role for the Lambda function"
  value       = aws_iam_role.earnings_lambda.name
}

output "github_actions_role_arn" {
  description = "Copy this ARN to GitHub Settings -> Secrets -> AWS_ROLE_ARN"
  value       = aws_iam_role.github_actions.arn
}
