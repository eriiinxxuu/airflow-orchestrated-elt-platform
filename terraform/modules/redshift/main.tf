data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# ── Security Group ────────────────────────────────────────────
resource "aws_security_group" "redshift" {
  name        = "yf-elt-redshift-sg-${var.environment}"
  description = "Redshift Serverless - inbound from VPC only"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Redshift port from VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "yf-elt-redshift-sg-${var.environment}"
  }
}

# ── IAM Role for Redshift to read S3 ─────────────────────────
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3" {
  name               = "yf-elt-redshift-s3-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "yf-elt-redshift-s3-policy-${var.environment}"
  role = aws_iam_role.redshift_s3.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Read"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          var.data_bucket_arn,
          "${var.data_bucket_arn}/*",
        ]
      },
    ]
  })
}

# ── Redshift Serverless Namespace ─────────────────────────────
# Namespace holds the database, users and schemas
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "yf-elt-${var.environment}"
  db_name             = var.database_name
  admin_username      = var.admin_username
  admin_user_password = var.admin_password
  iam_roles           = [aws_iam_role.redshift_s3.arn]

  lifecycle {
    ignore_changes = [admin_user_password]
  }
}

# ── Redshift Serverless Workgroup ─────────────────────────────
# Workgroup handles the compute and network configuration
resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "yf-elt-${var.environment}"
  base_capacity  = var.base_capacity

  subnet_ids         = var.private_subnet_ids
  security_group_ids = [aws_security_group.redshift.id]

  # Keep workgroup private — accessible from VPC only
  publicly_accessible = false
}
