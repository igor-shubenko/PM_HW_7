from datetime import timedelta

from airflow.models import DAG, TaskInstance
from airflow.operators.python import PythonOperator
from pendulum import today

from common.default_args import default_args


def _sample_callable(name: str) -> str:
    print(f"Hello from python callable {name}")
    return name


def _check_xcom(ti: TaskInstance) -> None:
    xcom_value = ti.xcom_pull(key="return_value", task_ids=["push_xcom"])
    print(f"XCOM value is: {xcom_value}")


with DAG(
        dag_id="dummy_xcom",
        default_args=default_args,
        description="Dummy DAG",
        schedule_interval=timedelta(days=1),
        start_date=today().add(days=-5),
        tags=["dummy"],
        catchup=True
) as dag:
    push_xcom = PythonOperator(
        task_id="push_xcom",
        python_callable=_sample_callable,
        op_kwargs={"name": "test_kwarg"}
    )

    check_xcom = PythonOperator(
        task_id="check_xcom",
        python_callable=_check_xcom,
    )

    push_xcom >> check_xcom
