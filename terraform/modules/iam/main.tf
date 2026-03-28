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
      {
        # Required for Lambda running inside a VPC
        # Lambda needs to create/delete network interfaces to attach to the VPC
        Sid    = "VPCNetworkInterface"
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:AssignPrivateIpAddresses",
          "ec2:UnassignPrivateIpAddresses",
        ]
        Resource = "*"
      },
    ]
  })
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ── GitHub OIDC Provider ──────────────────────────────────────
# Allows GitHub Actions to authenticate with AWS without storing
# long-lived credentials as GitHub secrets.
# Only needs to be created once per AWS account.
resource "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = ["sts.amazonaws.com"]

  # GitHub's OIDC thumbprint
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# ── Trust Policy ──────────────────────────────────────────────
# Only workflows from your specific repo can assume this role.
# The wildcard (*) at the end allows any branch/tag/PR.
data "aws_iam_policy_document" "github_assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_org}/${var.github_repo}:*"]
    }
  }
}

# ── GitHub Actions IAM Role ───────────────────────────────────
resource "aws_iam_role" "github_actions" {
  name               = "yf-elt-github-actions-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.github_assume.json
}

# ── Permissions for GitHub Actions ───────────────────────────

resource "aws_iam_role_policy_attachment" "github_actions_admin" {
  role       = aws_iam_role.github_actions.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}
# resource "aws_iam_role_policy" "github_actions" {
#   name = "yf-elt-github-actions-policy-${var.environment}"
#   role = aws_iam_role.github_actions.id

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Sid    = "TerraformStateS3"
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:PutObject",
#           "s3:DeleteObject",
#           "s3:ListBucket",
#           "s3:GetBucketLocation",
#           "s3:GetBucketPolicy",
#           "s3:PutBucketPolicy",
#           "s3:GetBucketVersioning",
#           "s3:GetBucketPublicAccessBlock",
#           "s3:GetEncryptionConfiguration",
#           "s3:GetLifecycleConfiguration",
#                   ]
#         Resource = [
#           "arn:aws:s3:::yf-elt-terraform-state*",
#           "arn:aws:s3:::yf-elt-terraform-state*/*",
#         ]
#       },
#       {
#         Sid    = "TerraformStateLock"
#         Effect = "Allow"
#         Action = [
#           "dynamodb:GetItem",
#           "dynamodb:PutItem",
#           "dynamodb:DeleteItem",
#         ]
#         Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/terraform-locks"
#       },
#       {
#         Sid    = "ECRAuth"
#         Effect = "Allow"
#         Action = ["ecr:GetAuthorizationToken"]
#         Resource = "*"
#       },
#       {
#         Sid    = "ECRPush"
#         Effect = "Allow"
#         Action = [
#           "ecr:BatchCheckLayerAvailability",
#           "ecr:GetDownloadUrlForLayer",
#           "ecr:BatchGetImage",
#           "ecr:PutImage",
#           "ecr:InitiateLayerUpload",
#           "ecr:UploadLayerPart",
#           "ecr:CompleteLayerUpload",
#           "ecr:DescribeRepositories",
#           "ecr:ListImages",
#         ]
#         Resource = "arn:aws:ecr:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:repository/yf-elt-*"
#       },
#       {
#         Sid    = "S3DAGs"
#         Effect = "Allow"
#         Action = [
#           "s3:PutObject",
#           "s3:GetObject",
#           "s3:DeleteObject",
#           "s3:ListBucket",
#           "s3:GetBucketLocation",
#         ]
#         Resource = [
#           "arn:aws:s3:::yf-elt-mwaa-*",
#           "arn:aws:s3:::yf-elt-mwaa-*/*",
#           "arn:aws:s3:::yf-elt-data-*",
#           "arn:aws:s3:::yf-elt-data-*/*",
#         ]
#       },
#       {
#         Sid    = "SSMRead"
#         Effect = "Allow"
#         Action = ["ssm:GetParameter"]
#         Resource = "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/yf-elt/*"
#       },
#       {
#         # Terraform needs broad IAM permissions to manage all the roles it creates
#         Sid    = "IAMManage"
#         Effect = "Allow"
#         Action = [
#           "iam:CreateRole",
#           "iam:DeleteRole",
#           "iam:GetRole",
#           "iam:ListRolePolicies",
#           "iam:ListAttachedRolePolicies",
#           "iam:PutRolePolicy",
#           "iam:DeleteRolePolicy",
#           "iam:GetRolePolicy",
#           "iam:AttachRolePolicy",
#           "iam:DetachRolePolicy",
#           "iam:PassRole",
#           "iam:CreateOpenIDConnectProvider",
#           "iam:GetOpenIDConnectProvider",
#           "iam:DeleteOpenIDConnectProvider",
#           "iam:TagOpenIDConnectProvider",
#         ]
#         Resource = "*"
#       },
#       {
#         Sid    = "TerraformGeneral"
#         Effect = "Allow"
#         Action = [
#           # VPC / Network
#           "ec2:*",
#           # ECS
#           "ecs:*",
#           "ecr:*",
#           # Lambda
#           "lambda:*",
#           # EventBridge
#           "events:*",
#           # CloudWatch
#           "cloudwatch:*",
#           "logs:*",
#           # SNS
#           "sns:*",
#           # Secrets Manager
#           "secretsmanager:*",
#           # MWAA
#           "airflow:*",
#           # Redshift Serverless
#           "redshift-serverless:*",
#           "redshift:*",
#         ]
#         Resource = "*"
#       },
#     ]
#   })
# }
