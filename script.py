"""
python3 script.py <YYYY-mm-dd>
Ex.
python3 script.py 2024-09-28
"""

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def process_logs(spark, target_date: str):
    target_date = datetime.strptime(target_date, "%Y-%m-%d")
    start_date = target_date - timedelta(days=7)
    logs_dir = "input"
    output_dir = "output"

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

    aggregated_data = all_logs.groupBy("email").agg(
        F.count(F.when(F.col("action") == "CREATE", True)).alias(
            "create_count"
        ),
        F.count(F.when(F.col("action") == "READ", True)).alias("read_count"),
        F.count(F.when(F.col("action") == "UPDATE", True)).alias(
            "update_count"
        ),
        F.count(F.when(F.col("action") == "DELETE", True)).alias(
            "delete_count"
        ),
    )

    output_file = os.path.join(output_dir, target_date.strftime("%Y-%m-%d"))
    aggregated_data.coalesce(1).write.csv(
        output_file, header=True, mode="overwrite"
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "User Actions Aggregation"
    ).getOrCreate()
    try:
        process_logs(spark, sys.argv[1])
    finally:
        spark.stop()
