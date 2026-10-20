# Spark: анализ MovieLens и ML по тегам

Контейнер загружает датасет MovieLens `ml-latest-small` в HDFS, запускает PySpark-приложение на YARN с двумя исполнителями и записывает результаты анализа в `/sparkExperiments.txt`. В образ входят Hadoop **3.3.6**, Spark **3.4.2** и PySpark **3.4.2**.

## Подключение

В `run_hw.sh` и `spark_job.py` сейчас указан адрес `192.168.34.2`:

- HDFS: `hdfs://192.168.34.2:8020`
- YARN ResourceManager: `192.168.34.2:8032`

Для окружения с DNS-именем `master` замените IP на `master` в обоих файлах. Убедитесь, что HDFS и YARN доступны контейнеру.

## Запуск

Из каталога репозитория:

```bash
docker build -t spark-homework ./spark
docker run --rm spark-homework
```

При старте контейнер ожидает HDFS, копирует `ratings.csv` и `tags.csv` в `/ml-latest-small` (если датасет ещё не загружен), создаёт `/sparkExperiments.txt` и запускает `spark-submit` с `--master yarn` и двумя исполнителями.

## Результаты

Приложение читает `ratings` и `tags`, вычисляет метрики и сохраняет их построчно в `/sparkExperiments.txt` в HDFS:

```text
stages:n tasks:m
filmsUnique:n usersUnique:m
goodRating:n
timeDifference:n
avgRating:n
rmse:n
```

Метрики соответствуют заданию:

- число стадий и задач при чтении таблиц;
- количество уникальных фильмов и пользователей в `ratings`;
- количество оценок не ниже `4.0`;
- средняя абсолютная разница между timestamp тега и оценки для совпадающих пользователя и фильма, в секундах;
- среднее пользовательских средних оценок;
- RMSE предсказания оценки моделью `TfidfVectorizer` + `SGDRegressor`, применённой к тегу через PySpark UDF.

Прочитать итоговый файл можно командой:

```bash
hdfs dfs -cat /sparkExperiments.txt
```
