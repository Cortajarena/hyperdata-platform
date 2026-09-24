FROM python:3.11-slim

WORKDIR /opt/dbt

RUN pip install --no-cache-dir dbt-core dbt-postgres

COPY . .

ENTRYPOINT ["dbt"]
