data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ── ECR Repository ────────────────────────────────────────────
resource "aws_ecr_repository" "extractor" {
  name                 = "yf-elt-extractor-${var.environment}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "extractor" {
  repository = aws_ecr_repository.extractor.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 10 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 10
      }
      action = { type = "expire" }
    }]
  })
}

# ── ECS Cluster ───────────────────────────────────────────────
resource "aws_ecs_cluster" "main" {
  name = "yf-elt-cluster-${var.environment}"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

# ── CloudWatch Log Group for ECS tasks ───────────────────────
resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/yf-elt-extractor-${var.environment}"
  retention_in_days = 30
}

# ── Security Group ────────────────────────────────────────────
# ECS tasks only need outbound — they pull from Yahoo Finance API
# and push to S3. No inbound traffic needed.
resource "aws_security_group" "ecs_task" {
  name        = "yf-elt-ecs-task-sg-${var.environment}"
  description = "ECS extractor tasks - outbound only"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound (Yahoo Finance API, S3, ECR)"
  }
}

# ── IAM: ECS Task Execution Role ──────────────────────────────
# Allows ECS to pull the image from ECR and write logs to CloudWatch
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "yf-elt-ecs-execution-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution_managed" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ── IAM: ECS Task Role ────────────────────────────────────────
# Runtime permissions for the extractor container:
#   - Write extracted NDJSON files to the S3 data bucket
resource "aws_iam_role" "ecs_task" {
  name               = "yf-elt-ecs-task-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "yf-elt-ecs-task-policy-${var.environment}"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DataWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.data_bucket_arn,
          "${var.data_bucket_arn}/*",
        ]
      },
    ]
  })
}

# ── ECS Task Definition ───────────────────────────────────────
resource "aws_ecs_task_definition" "extractor" {
  family                   = "yf-elt-extractor-${var.environment}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.task_cpu)
  memory                   = tostring(var.task_memory)
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "extractor"
    image     = "${aws_ecr_repository.extractor.repository_url}:latest"
    essential = true

    environment = [
      { name = "S3_BUCKET",   value = var.data_bucket_name },
      { name = "ENVIRONMENT", value = var.environment },
      { name = "LOG_LEVEL",   value = "INFO" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.ecs.name
        "awslogs-region"        = data.aws_region.current.name
        "awslogs-stream-prefix" = "extractor"
      }
    }
  }])
}
