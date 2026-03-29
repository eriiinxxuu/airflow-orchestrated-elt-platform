data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ── MWAA Bucket ───────────────────────────────────────────────
# Stores DAGs, plugins.zip and requirements.txt for MWAA to read.
# MWAA enforces that versioning must be enabled on this bucket.
resource "aws_s3_bucket" "mwaa" {
  bucket = "yf-elt-mwaa-${var.environment}-${data.aws_caller_identity.current.account_id}"

  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_s3_bucket_versioning" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa" {
  bucket = aws_s3_bucket.mwaa.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa" {
  bucket                  = aws_s3_bucket.mwaa.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Data Bucket ───────────────────────────────────────────────
# Stores raw NDJSON files written by ECS extractor tasks.
# Partitioned by year/month/day for efficient Redshift COPY.
resource "aws_s3_bucket" "data" {
  bucket = "yf-elt-data-${var.environment}-${data.aws_caller_identity.current.account_id}"

  lifecycle {
    prevent_destroy = false
  }
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Expire raw staging files after 90 days — already loaded into Redshift
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# ── Auto-zip plugins from plugins/operators ───────────────────
# Terraform automatically zips the plugins/operators folder.
# No need to manually run zip — just run terraform apply.
data "archive_file" "plugins" {
  type        = "zip"
  source_dir  = "${path.module}/plugins"
  output_path = "${path.module}/plugins.zip"
}

# ── Pre-create folder structure in MWAA bucket ────────────────
resource "aws_s3_object" "mwaa_folders" {
  for_each = toset(["dags/", "plugins/", "requirements/"])
  bucket   = aws_s3_bucket.mwaa.id
  key      = each.value
  content  = ""
}

resource "aws_s3_object" "plugins_zip" {
  bucket = aws_s3_bucket.mwaa.id
  key    = "plugins/plugins.zip"
  source = data.archive_file.plugins.output_path
  etag   = data.archive_file.plugins.output_md5
}

resource "aws_s3_object" "requirements_txt" {
  bucket = aws_s3_bucket.mwaa.id
  key    = "requirements/requirements.txt"
  source = "${path.module}/plugins/requirements.txt"
  etag   = filemd5("${path.module}/plugins/requirements.txt")
}
# ── Upload DAGs to MWAA bucket ───────────────────────────────
# Syncs all .py files from the dags/ directory.
# etag ensures Terraform re-uploads whenever a DAG file changes.
locals {
  dag_files = fileset("${path.module}/../../../dags", "*.py")
}

resource "aws_s3_object" "dags" {
  for_each = local.dag_files

  bucket = aws_s3_bucket.mwaa.id
  key    = "dags/${each.value}"
  source = "${path.module}/../../../dags/${each.value}"
  etag   = filemd5("${path.module}/../../../dags/${each.value}")
}

# ── SSM Parameters ────────────────────────────────────────────
# Store bucket names in SSM so GitHub Actions can retrieve them
# without hardcoding values in workflow files
resource "aws_ssm_parameter" "mwaa_bucket_name" {
  name  = "/yf-elt/${var.environment}/mwaa_bucket_name"
  type  = "String"
  value = aws_s3_bucket.mwaa.bucket
}

resource "aws_ssm_parameter" "data_bucket_name" {
  name  = "/yf-elt/${var.environment}/data_bucket_name"
  type  = "String"
  value = aws_s3_bucket.data.bucket
}
resource "aws_s3_object" "startup_script" {
  bucket = aws_s3_bucket.mwaa.id
  key    = "scripts/startup.sh"
  source = "${path.module}/../mwaa/startup.sh"
  etag   = filemd5("${path.module}/../mwaa/startup.sh")
}