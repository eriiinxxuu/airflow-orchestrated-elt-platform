"""
yf_config.py
─────────────
Shared configuration for all Yahoo Finance DAGs.
Extracted into a separate file to avoid repeating the same code in every DAG.

Why use Airflow Variables?
  Storing the watchlist and ECS config in Airflow Variables means operators
  can update them directly from the UI without redeploying any code.
"""

from __future__ import annotations

from airflow.models import Variable

# Default watchlist used as a fallback when the Airflow Variable does not exist
_DEFAULT_WATCHLIST = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "NVDA",
    "META",
    "TSLA",
    "JPM",
    "JNJ",
    "V",
]


def get_watchlist() -> list[str]:
    try:
        return Variable.get("yf_watchlist", deserialize_json=True)
    except Exception:
        return _DEFAULT_WATCHLIST


def get_s3_bucket() -> str:
    return Variable.get("s3_bucket")


def get_ecs_config() -> dict:
    """
    Read the ECS runtime configuration stored in an Airflow Variable.

    Returns a dict that can be unpacked directly into Operator kwargs
    using **get_ecs_config().
    """
    raw = Variable.get("ecs_config", deserialize_json=True)
    return {
        "cluster": raw["cluster_arn"],
        "task_definition": raw["task_definition"],
        "container_name": raw["container_name"],
        "subnets": (
            raw["subnets"]
            if isinstance(raw["subnets"], list)
            else raw["subnets"].split(",")
        ),
        "security_groups": [raw["security_group"]],
    }


# Jinja template for the S3 partition prefix used in S3ToRedshiftOperator s3_key.
# execution_date is the logical execution time provided by Airflow,
# not the current wall-clock time.
S3_PARTITION = (
    "year={{ execution_date.year }}/"
    "month={{ '{:02d}'.format(execution_date.month) }}/"
    "day={{ '{:02d}'.format(execution_date.day) }}/"
)
