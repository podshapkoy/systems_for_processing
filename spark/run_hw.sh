#!/bin/bash

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
HDFS_IP="192.168.34.2"

echo "${HDFS_IP} master" >> /etc/hosts

echo "Генерация конфигураций Hadoop"
cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${HDFS_IP}:8020</value>
  </property>
</configuration>
EOF

cat > $HADOOP_CONF_DIR/yarn-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${HDFS_IP}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>${HDFS_IP}:8032</value>
  </property>
</configuration>
EOF

echo "Ожидаем доступности HDFS"
while ! nc -z ${HDFS_IP} 8020; do sleep 1; echo -n "."; done; echo " HDFS доступен!"

hdfs dfsadmin -safemode wait || true

echo "Проверка наличия датасета в HDFS"
hadoop fs -test -e /ml-latest-small/ratings.csv
RET=$?
if [ $RET -eq 0 ]; then
    echo "Датасет уже существует в директории /ml-latest-small/"
else
    echo "Загружаем локальные данные в HDFS"
    hadoop fs -mkdir -p /ml-latest-small || true
    hadoop fs -put -f /app/ml-latest-small/ratings.csv /ml-latest-small/ || true
    hadoop fs -put -f /app/ml-latest-small/tags.csv /ml-latest-small/ || true
    echo "Датасет загружен в HDFS"
fi

yarn application -list 2>/dev/null | grep "LabSparkML" | awk '{print $1}' | while read app_id; do
    yarn application -kill $app_id
done || true

hadoop fs -touchz /sparkExperiments.txt || true

echo ">>> Запуск Spark Application <<<"
export PYSPARK_DRIVER_PYTHON=/opt/conda/envs/sparkenv/bin/python
export PYSPARK_PYTHON=./environment/bin/python

spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf spark.yarn.am.memory=512m \
  --archives /app/pyspark_env.tar.gz#environment \
  /app/spark_job.py

hadoop fs -cat /sparkExperiments.txt

exit 0
