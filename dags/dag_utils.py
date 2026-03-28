"""
dag_utils.py
─────────────
Shared callbacks and default args factory used across all DAGs.
"""
from __future__ import annotations

import logging
from datetime import timedelta

import boto3
from airflow.models import Variable

log = logging.getLogger(__name__)


# ── SNS notification ──────────────────────────────────────────
def send_sns_message(subject: str, message: str) -> None:
    """
    Publish a message to the SNS topic ARN stored in the
    Airflow Variable 'sns_topic_arn'.

    SNS will fan out to all subscribers (email, SMS, Lambda, etc.)
    configured on the topic — no webhook URL to manage.
    """
    topic_arn = Variable.get("sns_topic_arn")
    try:
        boto3.client("sns").publish(
            TopicArn=topic_arn, Subject=subject, Message=message,
        )
    except Exception as exc:
        log.error("SNS notification failed: %s", exc)


# ── Airflow callbacks ─────────────────────────────────────────
def on_failure_callback(context: dict) -> None:
    """Send an SNS alert when a task fails."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exc = context.get("exception", "unknown")
    log_url = context["task_instance"].log_url

    send_sns_message(
        subject=f"[Airflow] FAILED - {dag_id}.{task_id}",
        message=(
            f"DAG:   {dag_id}\n"
            f"Task:  {task_id}\n"
            f"Error: {exc}\n"
            f"Logs:  {log_url}"
        ),
    )


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Send an SNS alert when a task misses its SLA."""
    task_ids = ", ".join(t.task_id for t in blocking_task_list)

    send_sns_message(
        subject=f"[Airflow] SLA Miss - {dag.dag_id}",
        message=(f"DAG:   {dag.dag_id}\n" f"Tasks: {task_ids}"),
    )


# ── Default DAG args ──────────────────────────────────────────
def default_args(retries: int = 2) -> dict:
    """
    Shared default args for all DAGs.

    retry_delay + retry_exponential_backoff combination:
      1st retry after 5 min, 2nd after 10 min, 3rd after 20 min, etc.
    """
    return {
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=60),
        "sla": timedelta(hours=2),
        "on_failure_callback": on_failure_callback,
        "email_on_failure": False,
        "email_on_retry": False,
    }
