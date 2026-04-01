# MWAA Orchestrated ELT Platfrom

## 📚Introduction
An end-to-end ELT pipeline that extracts financial data from `Yahoo Finance` via REST API using **on-demand ECS Fargate Standalone Tasks**, loads it into Redshift Serverless, and transforms it into analytics-ready fact tables — fully automated with Airflow on MWAA and deployed via Terraform + GitHub Actions.

## 👀 Architecture



## 🔍 Project Structure

```
├── dags/                         # Airflow DAG definitions
│   ├── yf_daily_ohlcv.py
│   ├── yf_daily_fundamentals.py
│   ├── yf_event_earnings.py
│   ├── dag_utils.py              # SNS callbacks, default_args
│   └── yf_config.py              # shared config, ECS kwargs
├── docker/
│   ├── yahoo_extractor.py        # ECS entrypoint, 4 extraction modes
│   └── Dockerfile
├── tests/
│   ├── test_dags_e2e.py          # end-to-end DAG logic (mock data)
│   ├── test_operators.py         # DataQualityOperator unit tests
│   ├── test_yahoo_extractor.py   # extractor unit tests
│   └── test_earnings_trigger.py  # Lambda unit tests
└── terraform/
    ├── envs/                     # root module
    └── modules/
        ├── network/              # VPC, subnets, NAT Gateway
        ├── s3/                   # MWAA + data buckets, plugin upload
        ├── ecs/                  # ECR, cluster, task definition
        ├── mwaa/                 # MWAA environment, execution role
        ├── redshift/             # Serverless namespace + workgroup
        ├── iam/                  # Lambda role + GitHub OIDC role
        ├── sns/                  # alerts topic + email subscription
        ├── cloudwatch/           # log group + 5 alarms
        ├── lambda/               # earnings trigger function
        └── eventbridge/          # schedule rule + Lambda permission
```

## 🛠️ Technical Skills
- **Cloud & Infrastructure**: MWAA, ECS Fargate, ECR, Lambda, EventBridge, Redshift Serverless, CloudWatch, SNS
- **Data Engineering**: ELT pipelines, Data Quality Checks, Event-driven pipelines
- **DevOps**: Docker, Terraform, Github Actions, CI/CD
- **Programming**: Python, SQL, HCL (Terraform)


## CI/CD workflows
 
| Workflow | Trigger | Action |
|---------|---------|--------|
| `ci.yml` | All pushes / PRs | flake8, black auto-format, pytest (49 tests) |
| `push_image_ecr.yml` | `docker/` changes | Build, Trivy scan, push to ECR |
| `terraform.yml` | `terraform/` or `plugins/` changes | `terraform plan` on PR / `terraform apply` on main |
| `sync_dag_s3.yml` | `dags/` changes | `aws s3 sync` to MWAA bucket |

## DAG task chains
 
**yf_daily_ohlcv** and **yf_daily_fundamentals**:
```
extract (ECS Fargate) -> load_staging (S3 COPY) -> quality_checks -> transform (SQL)
```
 
**yf_event_earnings**:
```
parse_conf -> extract (ECS Fargate) -> load_staging -> quality_checks -> transform -> notify_sns
```


## Getting started
See [deployment.md](deployment.md) for full deployment instructions.
