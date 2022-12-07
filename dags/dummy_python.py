from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import today

from helpers.default_args import default_args


def _sample_callable(name: str) -> None:
    print(f"Hello from python callable {name}")


with DAG(
        dag_id="dummy_python",
        default_args=default_args,
        description="Dummy DAG",
        schedule_interval=timedelta(days=1),
        start_date=today().add(days=-5),
        tags=["dummy"],
        catchup=True
) as dag:
    task_a = PythonOperator(
        task_id="task_a",
        python_callable=_sample_callable,
        op_kwargs={'name': 'test_kwarg'}
    )
