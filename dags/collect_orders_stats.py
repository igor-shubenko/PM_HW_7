from datetime import timedelta
from pendulum import now, yesterday

from airflow import DAG

from helpers.default_args import default_args
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(dag_id='Collect_orders_stats',
         description='Collects orders data and inserts it into table',
         schedule_interval=timedelta(days=1),
         start_date=yesterday(),
         tags=['orders'],
         default_args=default_args,
         catchup=False) as dag:

    create_empty_table = PostgresOperator(task_id='Create_empty_table',
                                          sql='sql_scripts/create_table_order_status_stats.sql',
                                          postgres_conn_id='default_database')

    fill_orders_stats = PostgresOperator(task_id='Fill_orders_stats',
                                         sql='sql_scripts/fill_order_status_stats.sql',
                                         postgres_conn_id='default_database')

    create_empty_table >> fill_orders_stats
