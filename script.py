import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def process_logs(spark, target_date: str):
    # Параметры
    target_date = datetime.strptime(target_date, "%Y-%m-%d")
    start_date = target_date - timedelta(days=7)
    logs_dir = "logs"
    output_dir = "output"

    # Сбор логов за 7 предыдущих дней
    all_logs: DataFrame = spark.createDataFrame(
        [], schema="email STRING, action STRING, dt STRING"
    )

    for single_date in (start_date + timedelta(n) for n in range(7)):
        log_file = os.path.join(
            logs_dir, single_date.strftime("%Y-%m-%d") + ".csv"
        )
        if os.path.exists(log_file):
            daily_logs = spark.read.csv(log_file, header=True)
            all_logs = all_logs.union(daily_logs)

    # Агрегация данных
    aggregated_data = all_logs.groupBy("email").agg(
        F.count(F.when(F.col("action") == "create", True)).alias(
            "create_count"
        ),
        F.count(F.when(F.col("action") == "read", True)).alias("read_count"),
        F.count(F.when(F.col("action") == "update", True)).alias(
            "update_count"
        ),
        F.count(F.when(F.col("action") == "delete", True)).alias(
            "delete_count"
        ),
    )

    # Подготовка выходного файла
    output_file = os.path.join(
        output_dir, target_date.strftime("%Y-%m-%d") + ".csv"
    )
    aggregated_data.coalesce(1).write.csv(
        output_file, header=True, mode="overwrite"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
    else:
        spark = SparkSession.builder.appName(
            "User Actions Aggregation"
        ).getOrCreate()
        try:
            process_logs(spark, sys.argv[1])
        finally:
            spark.stop()
