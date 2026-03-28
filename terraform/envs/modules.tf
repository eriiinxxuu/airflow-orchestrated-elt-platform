# ── S3 ────────────────────────────────────────────────────────
# Two buckets: one for MWAA (DAGs/plugins), one for raw data
module "s3" {
  source      = "../modules/s3"
  environment = var.environment
}

# ── Network ───────────────────────────────────────────────────
module "network" {
  source      = "../modules/network"
  environment = var.environment
  cidr_block  = var.vpc_cidr
}

# ── ECS ───────────────────────────────────────────────────────
# Cluster + task definition + ECR repo + IAM roles for extractor
module "ecs" {
  source      = "../modules/ecs"
  environment = var.environment

  vpc_id           = module.network.vpc_id
  data_bucket_arn  = module.s3.data_bucket_arn
  data_bucket_name = module.s3.data_bucket_name
}

# ── SNS ───────────────────────────────────────────────────────
module "sns" {
  source      = "../modules/sns"
  environment = var.environment

  alert_email             = var.alert_email
  mwaa_execution_role_arn = module.mwaa.execution_role_arn
}

# ── MWAA ──────────────────────────────────────────────────────
module "mwaa" {
  source      = "../modules/mwaa"
  environment = var.environment

  s3_bucket_name                 = module.s3.mwaa_bucket_name
  s3_bucket_arn                  = module.s3.mwaa_bucket_arn
  data_bucket_arn                = module.s3.data_bucket_arn
  private_subnet_ids             = module.network.private_subnet_ids
  vpc_id                         = module.network.vpc_id
  allowed_cidr_blocks            = var.allowed_cidr_blocks
  environment_class              = var.mwaa_environment_class
  min_workers                    = var.mwaa_min_workers
  max_workers                    = var.mwaa_max_workers
  sns_topic_arn                  = module.sns.topic_arn
  plugins_s3_object_version      = module.s3.plugins_s3_object_version
  requirements_s3_object_version = module.s3.requirements_s3_object_version
}

# ── IAM ───────────────────────────────────────────────────────
module "iam" {
  source       = "../modules/iam"
  environment  = var.environment
  mwaa_env_arn = module.mwaa.environment_arn
}

# ── CloudWatch ────────────────────────────────────────────────
module "cloudwatch" {
  source      = "../modules/cloudwatch"
  environment = var.environment

  sns_topic_arn        = module.sns.topic_arn
  mwaa_env_name        = module.mwaa.environment_name
  ecs_cluster_name     = module.ecs.cluster_name
  lambda_function_name = module.lambda.function_name
}

# ── Lambda ────────────────────────────────────────────────────
module "lambda" {
  source      = "../modules/lambda"
  environment = var.environment

  lambda_role_arn    = module.iam.lambda_role_arn
  log_group_name     = module.cloudwatch.log_group_name
  airflow_base_url   = module.mwaa.webserver_url
  mwaa_env_name      = module.mwaa.environment_name
  watchlist          = var.watchlist
  days_ahead         = var.days_ahead
  subnet_ids         = module.network.private_subnet_ids
  security_group_ids = [module.network.lambda_security_group_id]
}

# ── Redshift ──────────────────────────────────────────────────
# Reads from S3 data bucket via COPY, serves SQL transformations
# for downstream analytics
module "redshift" {
  source      = "../modules/redshift"
  environment = var.environment

  vpc_id             = module.network.vpc_id
  private_subnet_ids = module.network.private_subnet_ids
  vpc_cidr           = module.network.vpc_cidr
  admin_username     = var.redshift_admin_username
  admin_password     = var.redshift_admin_password
  data_bucket_arn    = module.s3.data_bucket_arn
  sns_topic_arn      = module.sns.topic_arn
}

# ── EventBridge ───────────────────────────────────────────────
module "eventbridge" {
  source      = "../modules/eventbridge"
  environment = var.environment

  lambda_arn           = module.lambda.function_arn
  lambda_function_name = module.lambda.function_name
}
