"""
yf_config.py
─────────────
所有 Yahoo Finance DAG 共用的配置。
把它抽成独立文件，避免在每个 DAG 里重复同样的代码。

Airflow Variable 的好处：
  watchlist 和 ECS 配置存在 Airflow Variable 里，
  运维人员可以在 UI 里直接修改，不需要重新部署代码。
"""

from __future__ import annotations
from airflow.models import Variable

# 默认 watchlist（Variable 不存在时的回退值）
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
    读取存储在 Airflow Variable 里的 ECS 运行时配置。
    Terraform 通过 Secrets Manager → MWAA 自动同步这个 Variable。

    返回的 dict 可以直接用 **get_ecs_config() 展开到 Operator 参数中。
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


# Jinja 模板：S3 分区前缀，在 S3ToRedshiftOperator 的 s3_key 里使用
# execution_date 是 Airflow 传入的逻辑执行时间，不是当前时间
S3_PARTITION = (
    "year={{ execution_date.year }}/"
    "month={{ '{:02d}'.format(execution_date.month) }}/"
    "day={{ '{:02d}'.format(execution_date.day) }}/"
)
