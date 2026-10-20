FROM python:3.9-slim-bullseye

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    wget \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN pip install apache-flink==1.17.2

WORKDIR /app

RUN wget -q https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar -O /app/flink-sql-connector-kafka-1.17.2.jar

COPY run.sh /app/run.sh
COPY flink_job.py /app/flink_job.py

RUN sed -i 's/\r$//' /app/run.sh && chmod +x /app/run.sh

CMD ["/bin/bash", "/app/run.sh"]
