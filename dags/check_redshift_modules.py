from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_redshift_modules():
    import airflow.providers.amazon.aws.operators as ops
    import pkgutil
    for mod in pkgutil.iter_modules(ops.__path__):
        if 'redshift' in mod.name:
            print(f"Found: {mod.name}")

with DAG(
    dag_id="debug_check_modules",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="check_modules",
        python_callable=check_redshift_modules,
    )
    