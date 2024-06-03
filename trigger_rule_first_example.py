from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

with DAG(
    "trigger_rule_first_example",
    start_date=datetime(2024,1,1),
    schedule_interval='@once',
    catchup=False
    ) as dag:
    
    task_a = DummyOperator(
        task_id="task_a"
    )

    task_b = DummyOperator(
        task_id="task_b"
    )

    task_c = BashOperator(
        task_id="task_c",
        bash_command="echo Ready",
        trigger_rule="all_success"
    )

    [task_a, task_b] >> task_c