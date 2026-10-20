FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HADOOP_VERSION=3.3.6
ENV SPARK_VERSION=3.4.2
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=/opt/conda/envs/sparkenv/bin:/opt/conda/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$JAVA_HOME/bin
ENV HADOOP_USER_NAME=root

RUN apt-get update && apt-get install -y \
    openjdk-8-jdk-headless \
    wget \
    net-tools \
    iputils-ping \
    netcat \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz

RUN wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p /opt/conda && \
    rm /tmp/miniconda.sh

RUN /opt/conda/bin/conda create -y -n sparkenv -c conda-forge --override-channels python=3.9 pandas scikit-learn requests conda-pack pip && \
    /opt/conda/bin/conda run -n sparkenv pip install pyspark==${SPARK_VERSION}

WORKDIR /app

RUN /opt/conda/bin/conda run -n sparkenv conda-pack -o /app/pyspark_env.tar.gz

RUN wget -q https://files.grouplens.org/datasets/movielens/ml-latest-small.zip -O /tmp/ml.zip && \
    unzip /tmp/ml.zip -d /app/ && \
    rm /tmp/ml.zip

COPY run_hw.sh /app/run_hw.sh
COPY spark_job.py /app/spark_job.py

RUN sed -i 's/\r$//' /app/run_hw.sh && chmod +x /app/run_hw.sh

CMD ["/bin/bash", "/app/run_hw.sh"]
