from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import yesterday

from common.default_args import default_args

with DAG(
        dag_id="dummy_dag",
        default_args=default_args,
        description="Dummy DAG",
        schedule_interval=timedelta(days=1),
        start_date=yesterday(),
        tags=["dummy"],
        catchup=False,
) as dag:
    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_c = EmptyOperator(task_id="task_c")
    task_d = EmptyOperator(task_id="task_d")

    task_a >> [task_b, task_c] >> task_d
