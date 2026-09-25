FROM python:3.11-slim

WORKDIR /opt/dbt

RUN pip install --no-cache-dir \
    dbt-core==1.12.5 \
    dbt-postgres==1.11.0

COPY . .

ENTRYPOINT ["dbt"]
