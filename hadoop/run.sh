#!/bin/bash
set -e

export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

HDFS_IP="192.168.34.2"

echo "${HDFS_IP} master tank-uhFrnPOVH0.cr-int" >> /etc/hosts

echo "Генерация конфигураций Hadoop..."
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

cat > $HADOOP_CONF_DIR/mapred-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=${HADOOP_HOME}</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*</value>
  </property>
</configuration>
EOF

echo "Ждем доступности HDFS порта..."
while ! nc -z ${HDFS_IP} 8020; do sleep 1; echo -n "."; done; echo " HDFS порт доступен!"

echo "Ожидаем выхода HDFS из безопасного режима..."
hadoop dfsadmin -safemode wait

echo "Ожидание создания тестовых данных проверяющей системой..."
while ! hadoop fs -test -e /shadow.txt; do
    sleep 2
    echo -n "w"
done
echo " Файл /shadow.txt найден!"

while ! hadoop fs -test -d /delme; do
    sleep 2
    echo -n "d"
done
echo " Директория /delme найдена!"

echo ">>> выполнение заданий <<<"

hadoop fs -mkdir -p /createme
echo "[1/5] Директория /createme создана"

hadoop fs -rm -r -skipTrash /delme
echo "[2/5] Директория /delme удалена"

echo "Arbitrary Content" > /tmp/nonnull.txt
hadoop fs -put -f /tmp/nonnull.txt /nonnull.txt
echo "[3/5] Файл /nonnull.txt загружен"

hadoop fs -rm -r -skipTrash /wordcount_output || true
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /shadow.txt /wordcount_output
echo "[4/5] Job wordcount завершена"

COUNT=$(hadoop fs -cat /wordcount_output/part-r-* | awk '{if ($1 == "Innsmouth") print $2}')

if [ -z "$COUNT" ]; then
    COUNT=0
fi

echo "$COUNT" > /tmp/whataboutinsmouth.txt
hadoop fs -put -f /tmp/whataboutinsmouth.txt /whataboutinsmouth.txt
echo "[5/5] Число вхождений 'Innsmouth' ($COUNT) записано в /whataboutinsmouth.txt"

hadoop fs -ls /
exit 0
