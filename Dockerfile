FROM apache/airflow:2.3.3-python3.10

WORKDIR /opt/airflow

USER root
RUN apt-get update -q \
    && apt-get install --no-install-recommends --yes \
      gcc \
      libssl-dev \
      libsasl2-dev \
      libldap2-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN pip install --user --upgrade --no-cache-dir pip==22.1.2 poetry==1.1.13

COPY pyproject.toml .
COPY poetry.lock .

RUN poetry config virtualenvs.create false && \
    poetry install --no-dev
