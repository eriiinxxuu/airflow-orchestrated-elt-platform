# Deployment Guide

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.7
- Docker
- Python 3.11+

---

## Step 1 — Bootstrap Terraform state backend

Terraform needs an S3 bucket and DynamoDB table before it can manage any resources.
This is a one-time manual setup.

```bash
# Create S3 bucket for Terraform state
aws s3 mb s3://yf-elt-terraform-state --region ap-southeast-2

# Enable versioning so you can recover from accidental state corruption
aws s3api put-bucket-versioning \
  --bucket yf-elt-terraform-state \
  --versioning-configuration Status=Enabled

# Create DynamoDB table for state locking (prevents concurrent applies)
aws dynamodb create-table \
  --table-name terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-southeast-2
```

---

## Step 2 — Configure Terraform variables

```bash
cd terraform/envs
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
environment             = "dev"
aws_region              = "ap-southeast-2"
github_org              = "your-github-username"
github_repo             = "airflow-orchestrated-elt-platform"
redshift_admin_password = "YourSecurePassword123!"
alert_email             = "your@email.com"
```

---

## Step 3 — Deploy all modules except MWAA

MWAA reads `startup.sh` on first boot, so we need the ECS, Redshift, and SNS
outputs before deploying MWAA. Deploy all other modules first:

```bash
cd terraform/envs

terraform init

terraform apply \
  -target=module.network \
  -target=module.s3 \
  -target=module.ecs \
  -target=module.redshift \
  -target=module.sns \
  -target=module.iam \
  -target=module.cloudwatch \
  -target=module.lambda \
  -target=module.eventbridge
  -target=module.secrets
```

---

## Step 4 — Update startup.sh

MWAA runs `startup.sh` on every environment startup to automatically set all
Airflow Variables and the Redshift connection.

First, get the values from Terraform outputs:

```bash
cd terraform/envs

terraform output ecs_cluster_arn
terraform output ecs_task_definition_arn
terraform output ecs_task_security_group_id
terraform output private_subnet_ids
terraform output sns_topic_arn
```

Then create the Secrets Manager secret for the Redshift password so it exists
when MWAA starts up:

```bash
aws secretsmanager create-secret \
  --name yf-elt/redshift-password \
  --secret-string "YourSecurePassword123!" \
  --region ap-southeast-2
```

Also update the `SecretsManager` IAM statement in `terraform/modules/mwaa/main.tf`
to allow the MWAA execution role to read this secret:

```hcl
Resource = [
  "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:airflow/*",
  "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:yf-elt/*",
]
```

Now edit `terraform/modules/mwaa/startup.sh` with your actual values:

```bash
#!/bin/bash

airflow variables set s3_bucket "yf-elt-data-dev-{your-account-id}"

airflow variables set ecs_config '{
  "cluster_arn": "arn:aws:ecs:ap-southeast-2:{account-id}:cluster/yf-elt-cluster-dev",
  "task_definition": "arn:aws:ecs:ap-southeast-2:{account-id}:task-definition/yf-elt-extractor-dev:1",
  "container_name": "extractor",
  "subnets": ["subnet-xxx", "subnet-yyy"],
  "security_group": "sg-xxx"
}'

airflow variables set sns_topic_arn "arn:aws:sns:ap-southeast-2:{account-id}:yf-elt-airflow-alerts-dev"

# Read Redshift password from Secrets Manager and create the Airflow connection.
# Using Secrets Manager means the password is never stored in plaintext on S3.
REDSHIFT_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id yf-elt/redshift-password \
  --region ap-southeast-2 \
  --query SecretString \
  --output text)

airflow connections add redshift_default \
  --conn-type redshift \
  --conn-host "$(aws redshift-serverless get-workgroup \
    --workgroup-name yf-elt-dev \
    --query 'workgroup.endpoint.address' \
    --output text)" \
  --conn-login admin \
  --conn-password "$REDSHIFT_PASSWORD" \
  --conn-port 5439 \
  --conn-schema yf_elt || true
```

The `|| true` prevents the script from failing if the connection already exists
on subsequent MWAA restarts.

---

## Step 5 — Deploy MWAA

Now that `startup.sh` and the Secrets Manager secret are both ready, run the
full apply. Terraform will upload `startup.sh` to S3 and deploy MWAA in a
single step:

```bash
cd terraform/envs
terraform apply
```

> MWAA deployment takes 30–40 minutes. After it becomes `AVAILABLE`, the
> startup script runs automatically and sets all Airflow Variables and the
> Redshift connection.

Confirm everything was set at **Admin → Variables** and **Admin → Connections**
in the Airflow UI.

---

## Step 6 — Set up GitHub Actions secrets

```bash
terraform output github_actions_role_arn
```

In your GitHub repository go to **Settings → Secrets and variables → Actions** and add:

| Secret | Value |
|--------|-------|
| `AWS_ROLE_ARN` | output from `terraform output github_actions_role_arn` |
| `REDSHIFT_PASSWORD` | same password used in `terraform.tfvars` |
| `ALERT_EMAIL` | your alert email address |

---

## Step 7 — Build and push Docker image

The `push_image_ecr.yml` workflow triggers automatically when `docker/` files change.
To trigger it manually, push any change to `docker/` or use `workflow_dispatch`
in the GitHub Actions UI.

Alternatively, build and push locally:

```bash
aws ecr get-login-password --region ap-southeast-2 | \
  docker login --username AWS --password-stdin \
  $(terraform output -raw ecr_repository_url)

docker build -t yf-elt-extractor-dev docker/
docker tag yf-elt-extractor-dev:latest \
  $(terraform output -raw ecr_repository_url):latest
docker push $(terraform output -raw ecr_repository_url):latest
```

---

## Step 8 — Verify

```bash
aws mwaa get-environment \
  --name yf-elt-airflow-dev \
  --query "Environment.Status" \
  --output text
# Expected: AVAILABLE
```

In the Airflow UI:

- **DAGs** — 3 DAGs with no import errors
- **Admin → Variables** — `s3_bucket`, `ecs_config`, `sns_topic_arn` all present
- **Admin → Connections** — `redshift_default` present
- **Admin → Providers** — `apache-airflow-providers-amazon == 8.24.0`

To run a test:

```
Airflow UI -> yf_daily_ohlcv -> Trigger DAG
```

---

## Teardown

```bash
cd terraform/envs

# Empty S3 buckets first (Terraform will not delete non-empty buckets)
aws s3 rm s3://yf-elt-data-dev-{account-id} --recursive
aws s3 rm s3://yf-elt-mwaa-dev-{account-id} --recursive

terraform destroy
```

---

## Troubleshooting

**Yahoo Finance 429 rate limiting**

The Yahoo Finance API is unofficial and rate-limits by IP. ECS Fargate tasks
use the NAT Gateway IP, which may be temporarily blocked after heavy testing.
Wait 1–2 hours or schedule runs during off-peak hours. For production use,
consider switching to the `Polygon.io extractor`
