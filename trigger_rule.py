# 1st pipeline:
## task_a | success
## task_b | success
# 2nd pipeline:
## task_a | failed
## tasl_b | failed
# 3rd pipeline:
# task_a | skipped
# task_b | skipped
# 4th pipeline:
## task_a | failed
## task_b | success
# 5th pipeline:
## task_a | skipped
## task_b | success
# 6th pipeline:
## task_a | skipped
## task_b | failed

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSkipException

from datetime import datetime
import time

trigger_rule_value="all_success"
#trigger_rule_value="all_failed"
#trigger_rule_value="all_done"
#trigger_rule_value="all_skipped"
#trigger_rule_value="one_success"
#trigger_rule_value="one_failed"
#trigger_rule_value="one_done"
#trigger_rule_value="none_failed"
#trigger_rule_value="none_failed_min_one_success"
#trigger_rule_value="none_skipped"
#trigger_rule_value="always"

def skipped_function():
    time.sleep(5)
    raise AirflowSkipException("Skipped")

def failed_function():
    time.sleep(5)
    raise ValueError("Failed")

def print_function():
    print("Ready")
    return True

with DAG(
    "trigger_rule",
    start_date=datetime(2024,1,1),
    schedule_interval='@once',
    catchup=False
    ) as dag:
    
    # 1st pipeline ====================================
    task_a_1 = DummyOperator(
        task_id="task_a_1"
    )

    task_b_1 = DummyOperator(
        task_id="task_b_1"
    )

    task_c_1 = PythonOperator(
        task_id="task_c_1",
        python_callable=print_function,
        trigger_rule=trigger_rule_value
    )

    # 2nd pipeline ====================================
    task_a_2 = PythonOperator(
        task_id="task_a_2",
        python_callable=failed_function
    )

    task_b_2 = PythonOperator(
        task_id="task_b_2",
        python_callable=failed_function
    )

    task_c_2 = PythonOperator(
        task_id="task_c_2",
        python_callable=print_function,
        trigger_rule=trigger_rule_value
    )

    # 3rd pipeline ====================================
    task_a_3 = PythonOperator(
        task_id="task_a_3",
        python_callable=skipped_function
    )

    task_b_3 = PythonOperator(
        task_id="task_b_3",
        python_callable=skipped_function
    )

    task_c_3 = PythonOperator(
        task_id="task_c_3",
        python_callable=print_function,
        trigger_rule=trigger_rule_value
    )

    # 4th pipeline ====================================
    task_a_4 = DummyOperator(
        task_id="task_a_4"
    )

    task_b_4 = PythonOperator(
        task_id="task_b_4",
        python_callable=failed_function
    )

    task_c_4 = PythonOperator(
        task_id="task_c_4",
        python_callable=print_function,
        trigger_rule=trigger_rule_value
    )

    # 5th pipeline ====================================
    task_a_5 = DummyOperator(
        task_id="task_a_5"
    )

    task_b_5 = PythonOperator(
        task_id="task_b_5",
        python_callable=skipped_function
    )

    task_c_5 = PythonOperator(
        task_id="task_c_5",
        python_callable=print_function,
        trigger_rule=trigger_rule_value
    )

    # 6th pipeline ====================================
    task_a_6 = PythonOperator(
        task_id="task_a_6",
        python_callable=failed_function
    )

    task_b_6 = PythonOperator(
        task_id="task_b_6",
        python_callable=skipped_function
    )

    task_c_6 = PythonOperator(
        task_id="task_c_6",
        python_callable=print_function,
        trigger_rule=trigger_rule_value
    )

    [task_a_1, task_b_1] >> task_c_1
    [task_a_2, task_b_2] >> task_c_2
    [task_a_3, task_b_3] >> task_c_3
    [task_a_4, task_b_4] >> task_c_4
    [task_a_5, task_b_5] >> task_c_5
    [task_a_6, task_b_6] >> task_c_6