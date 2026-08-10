FROM apache/spark:3.5.3-java17-python3

USER root
WORKDIR /app

COPY requirements.txt ./requirements.txt
# The Spark base image already contains the matching PySpark runtime. Install
# only the project-level Python dependencies to avoid downloading PySpark twice.
# PySpark is supplied by the Spark base image; install the project and notebook
# dependencies without downloading a second Spark distribution.
RUN pip install --no-cache-dir \
    PyYAML==6.0.2 \
    requests==2.32.3 \
    pytest==8.3.3 \
    jupyterlab==4.3.5 \
    pandas==2.2.3 \
    matplotlib==3.9.2

COPY config ./config
COPY src ./src
COPY data ./data
COPY notebooks ./notebooks

RUN mkdir -p /app/events \
    && chown -R ${spark_uid}:${spark_gid} /app

ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    KONFIO_ICEBERG_STORAGE_TYPE=minio \
    KONFIO_ICEBERG_WAREHOUSE_PATH=s3a://konfio-warehouse/ \
    KONFIO_ICEBERG_S3_ENDPOINT=http://minio:9000 \
    KONFIO_ICEBERG_S3_ACCESS_KEY_ID=minioadmin \
    KONFIO_ICEBERG_S3_SECRET_ACCESS_KEY=minioadmin \
    KONFIO_ICEBERG_S3_PATH_STYLE_ACCESS=true \
    KONFIO_OUTPUT_EVENTS_PATH=/app/events

USER ${spark_uid}

CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4", "/app/src/main.py"]
