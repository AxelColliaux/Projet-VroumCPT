FROM apache/spark:3.5.3
USER root
RUN pip install --no-cache-dir pyspark==3.5.3
WORKDIR /app
COPY streaming_analysis.py .
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
    "/app/streaming_analysis.py"]
