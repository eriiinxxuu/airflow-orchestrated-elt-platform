data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ── Security Group ────────────────────────────────────────────
resource "aws_security_group" "mwaa" {
  name        = "yf-elt-mwaa-sg-${var.environment}"
  description = "Security group for MWAA environment"
  vpc_id      = var.vpc_id

  # MWAA workers need to communicate with each other
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  # Allow HTTPS inbound from specified CIDRs (office/VPN for webserver access)
  dynamic "ingress" {
    for_each = length(var.allowed_cidr_blocks) > 0 ? [1] : []
    content {
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = var.allowed_cidr_blocks
      description = "HTTPS access to Airflow webserver"
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }

  tags = {
    Name = "yf-elt-mwaa-sg-${var.environment}"
  }
}

# ── IAM Role ──────────────────────────────────────────────────
data "aws_iam_policy_document" "mwaa_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "mwaa_execution" {
  name               = "yf-elt-mwaa-execution-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.mwaa_assume.json
}

resource "aws_iam_role_policy" "mwaa_execution" {
  name = "yf-elt-mwaa-execution-policy-${var.environment}"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AirflowS3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetAccountPublicAccessBlock",
          "s3:GetBucketPublicAccessBlock",
          "s3:GetBucketAcl",
          "s3:ListAllMyBuckets",
        ]
        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*",
          var.data_bucket_arn,
          "${var.data_bucket_arn}/*",
          "*",
        ]
      },
      {
        Sid    = "AirflowLogging"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults",
          "logs:DescribeLogGroups",
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:airflow-*"
      },
      {
        Sid    = "AirflowMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = "*"
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = var.sns_topic_arn
      },
      {
        Sid    = "SecretsManager"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
        ]
        # MWAA reads Airflow connections/variables from Secrets Manager
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:airflow/*"
      },
      {
        Sid    = "KMSDecrypt"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey*",
          "kms:Encrypt",
        ]
        Resource    = "*"
        Condition = {
          StringLike = {
            "kms:ViaService" = [
              "s3.${data.aws_region.current.name}.amazonaws.com",
              "sqs.${data.aws_region.current.name}.amazonaws.com",
            ]
          }
        }
      },
      {
        Sid    = "SQSAccess"
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
        ]
        # MWAA uses SQS internally for task queuing
        Resource = "arn:aws:sqs:${data.aws_region.current.name}:*:airflow-celery-*"
      },
      {
        Sid    = "ECSRunTask"
        Effect = "Allow"
        Action = [
          "ecs:RunTask",
          "ecs:DescribeTasks",
          "ecs:StopTask",
          "ecs:DescribeTaskDefinition",
          "ecs:TagResource",
        ]
        Resource = "*"
      },
      {
        Sid    = "PassRoleToECS"
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = "*"
        Condition = {
          StringLike = {
            "iam:PassedToService" = "ecs-tasks.amazonaws.com"
          }
        }
      },
      {
        Sid    = "RedshiftDataAPI"
        Effect = "Allow"
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:GetStatementResult",
          "redshift-data:ListStatements",
          "redshift:GetClusterCredentials",
          "redshift:DescribeClusters",
        ]
        Resource = "*"
      },
    ]
  })
}

# ── CloudWatch Log Groups ─────────────────────────────────────
resource "aws_cloudwatch_log_group" "dag_processing" {
  name              = "airflow-${var.environment}-DAGProcessing"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "scheduler" {
  name              = "airflow-${var.environment}-Scheduler"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "task" {
  name              = "airflow-${var.environment}-Task"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "webserver" {
  name              = "airflow-${var.environment}-WebServer"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "airflow-${var.environment}-Worker"
  retention_in_days = 30
}

# ── MWAA Environment ──────────────────────────────────────────
resource "aws_mwaa_environment" "main" {
  name               = "yf-elt-airflow-${var.environment}"
  airflow_version    = var.airflow_version
  environment_class  = var.environment_class
  min_workers        = var.min_workers
  max_workers        = var.max_workers
  schedulers         = var.schedulers
  execution_role_arn = aws_iam_role.mwaa_execution.arn
  

  # S3 sources
  source_bucket_arn    = var.s3_bucket_arn
  dag_s3_path          = var.dag_s3_path
  plugins_s3_path      = var.plugins_s3_path
  requirements_s3_path = var.requirements_s3_path
  requirements_s3_object_version  = var.requirements_s3_object_version
  startup_script_s3_path = "scripts/startup.sh"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = var.private_subnet_ids
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "WARNING"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "WARNING"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  # Airflow reads connections and variables from Secrets Manager
  # when the key matches the pattern airflow/connections/* or airflow/variables/*
  airflow_configuration_options = merge(
    {
      "secrets.backend"                      = "airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
      "secrets.backend_kwargs"               = jsonencode({
        connections_prefix = "airflow/connections"
        variables_prefix   = "airflow/variables"
        profile_name       = null
      })
      "core.load_examples"                   = "false"
      "core.dagbag_import_timeout"           = "120"
      "scheduler.dag_dir_list_interval"      = "30"
    },
    var.airflow_configuration_options
  )

  webserver_access_mode = var.webserver_access_mode

  depends_on = [
    aws_cloudwatch_log_group.dag_processing,
    aws_cloudwatch_log_group.scheduler,
    aws_cloudwatch_log_group.task,
    aws_cloudwatch_log_group.webserver,
    aws_cloudwatch_log_group.worker,
    var.plugins_s3_object_version,
    var.requirements_s3_object_version,
    var.startup_script_s3_object_version,
  ]
}
