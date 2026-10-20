#!/bin/bash
set -e

KAFKA_IP="192.168.34.2"
echo "Ожидаем доступности Kafka брокера..."
while ! nc -z $KAFKA_IP 9092; do sleep 1; echo -n "."; done; echo " Kafka брокер доступен!"

echo ">>> Запуск PyFlink Streaming Job <<<"
python /app/flink_job.py

exit 0
