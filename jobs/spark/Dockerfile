FROM apache/spark:3.5.1

WORKDIR /opt/spark

# Workload code mounted/submitted at runtime; the directory is copied for
# simplicity of the local dev loop (see docker-compose.yml spark-master).
COPY ./ /opt/spark/workloads
