data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "earnings_lambda" {
  name               = "yf-earnings-trigger-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "earnings_lambda" {
  name = "yf-earnings-trigger-policy-${var.environment}"
  role = aws_iam_role.earnings_lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Sid      = "MWAAToken"
        Effect   = "Allow"
        Action   = ["airflow:CreateWebLoginToken"]
        Resource = var.mwaa_env_arn
      },
    ]
  })
}
