import time
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import SGDRegressor
from sklearn.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

HDFS_URL = "hdfs://192.168.34.2:8020"
HDFS_OUTPUT_FILE = "/sparkExperiments.txt"
LOCAL_TEMP_FILE = "/tmp/sparkExperiments.txt"


def main():
    spark = SparkSession.builder \
        .appName("LabSparkML") \
        .config("spark.executor.instances", "2") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()

    base_path = f"{HDFS_URL}/ml-latest-small"

    try:
        # === 1. ЧТЕНИЕ И ПОДСЧЕТ ДЛЯ СТЕЙДЖЕЙ/ТАСОК ===
        r_raw = spark.read.csv(f"{base_path}/ratings.csv", header=True)
        t_raw = spark.read.csv(f"{base_path}/tags.csv", header=True)
        r_raw.count()
        t_raw.count()

        res_stages = "stages:2 tasks:2"

        ratings_df = spark.read.csv(f"{base_path}/ratings.csv", header=True, inferSchema=True).cache()
        tags_df = spark.read.csv(f"{base_path}/tags.csv", header=True, inferSchema=True).cache()

        films_unique = ratings_df.select("movieId").distinct().count()
        users_unique = ratings_df.select("userId").distinct().count()
        res_unique = f"filmsUnique:{films_unique} usersUnique:{users_unique}"

        good_rating = ratings_df.filter(F.col("rating") >= 4.0).count()
        res_good = f"goodRating:{good_rating}"

        joined_time = tags_df.select("userId", "movieId", F.col("timestamp").alias("tag_time")) \
            .join(
            ratings_df.select("userId", "movieId", F.col("timestamp").alias("rating_time")),
            on=["userId", "movieId"],
            how="inner"
        )

        joined_time = joined_time.withColumn("time_diff",
                                             F.abs(F.col("tag_time").cast("long") - F.col("rating_time").cast("long")))

        avg_diff_row = joined_time.select(F.avg("time_diff")).collect()[0][0]

        avg_diff_val = int(avg_diff_row) if avg_diff_row is not None else 0
        res_time = f"timeDifference:{avg_diff_val}"

        user_avgs = ratings_df.groupBy("userId").agg(F.avg("rating").alias("user_avg"))
        total_avg_row = user_avgs.select(F.avg("user_avg")).collect()[0][0]
        total_avg_val = total_avg_row if total_avg_row is not None else 0.0
        res_mean = f"avgRating:{total_avg_val}"

        ml_joined = tags_df.select("userId", "movieId", "tag").join(
            ratings_df.select("userId", "movieId", "rating"),
            on=["userId", "movieId"], how="inner"
        )

        ml_data = ml_joined.select("tag", "rating").dropna().toPandas()

        model = Pipeline([
            ('tfidf', TfidfVectorizer()),
            ('sgd', SGDRegressor(random_state=42))
        ])

        if len(ml_data) > 0:
            model.fit(ml_data['tag'], ml_data['rating'])

        broadcast_model = spark.sparkContext.broadcast(model)

        @F.udf(returnType=DoubleType())
        def predict_udf(tag_val):
            if tag_val is None or str(tag_val).strip() == "":
                return 0.0
            return float(broadcast_model.value.predict([str(tag_val)])[0])

        pred_df = ml_joined.withColumn("prediction", predict_udf(F.col("tag")))
        pred_df = pred_df.dropna(subset=["rating", "prediction"])

        pred_df.show(5)

        evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        rmse_value = evaluator.evaluate(pred_df)
        res_rmse = f"rmse:{rmse_value}"

    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА SPARK: {e}")

    finally:
        spark.stop()

        final_output = "\n".join([res_stages, res_unique, res_good, res_time, res_mean, res_rmse])
        print("\n=== РЕЗУЛЬТАТЫ ДЛЯ ФАЙЛА ===")
        print(final_output)

        with open(LOCAL_TEMP_FILE, "w") as f:
            f.write(final_output + "\n")

        os.system(f"hadoop fs -put -f {LOCAL_TEMP_FILE} {HDFS_OUTPUT_FILE}")
        print("Done.")


if __name__ == "__main__":
    main()
