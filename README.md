# PM Academy Apache Airflow practice

## Quick start guide

To start airflow run following command:
```shell
docker compose up
```

Airflow credentials: ``airflow/airflow``.

To stop airflow press ```Ctrl + C```.  

To stop and remove airflow containers run following command in extra terminal:
```shell
docker compose down
```

To create virtual env run following commands:
```shell
poetry install
source .venv/bin/activate
```

To connect to postgres instance with data use following credentials:
```yaml
host: localhost
port: 5438  # set it to 5432 for Airflow Connection
user: postgres
password: postgres
```

## Homework

### Problem statement:
1. Create table ``order_status_stats`` with columns ``dt date, order_status_name varchar(100), orders_count int``
2. Fill the table with orders count aggregated from ``sale`` table and grouped by date and order status name.

### Acceptance criteria
1. Table ``order_status_stats`` is created via docker compose or inside airflow DAG.
2. Table ``order_status_stats`` is filled with data with Airflow DAG.
3. Fork this repo to you private one.
4. Create MR to ``master`` branch with all required scripts and docs.

### Tech tips:
- You may use [Airflow Postgres Operator](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html).
- Pay attention that database port for connection from Airflow should be ``5432``.
