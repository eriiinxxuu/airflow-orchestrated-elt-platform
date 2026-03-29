# MWAA Orchestrated ELT Platfrom

## 📚Introduction
An end-to-end ELT pipeline that extracts financial data from Yahoo Finance via REST API, loads it into Redshift Serverless, and transforms it into analytics-ready fact tables — fully automated with Airflow on MWAA and deployed via Terraform + GitHub Actions.

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

## Getting started
See [deployment.md](deployment.md) for full deployment instructions.
