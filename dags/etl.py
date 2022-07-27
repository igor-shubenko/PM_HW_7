from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import yesterday

from common.default_args import default_args

create_table_sql = """
DROP TABLE IF EXISTS order_status_stats;
CREATE TABLE IF NOT EXISTS order_status_stats (
    status_name varchar(100),
    cnt int
);
"""

insert_sql = """
INSERT INTO order_status_stats SELECT
    status_name,
    count(*) as cnt
FROM order_status os
JOIN status_name sn USING (status_name_id)
GROUP BY 1
"""

with DAG(
        "etl",
        default_args=default_args,
        description="An example DAG demonstrating the usage of DataHub's Airflow lineage backend.",
        schedule_interval=timedelta(days=1),
        start_date=yesterday(),
        tags=["example_tag"],
        catchup=False,
) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_data",
        sql=create_table_sql
    )

    insert = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres_data",
        sql=insert_sql,
    )

    create_table >> insert
