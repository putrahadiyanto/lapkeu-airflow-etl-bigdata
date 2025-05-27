from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import logging

def create_spark_session():
    """
    Create a spark session with MongoDB connector configuration
    """
    CURRENT_YEAR_STR = str(datetime.now().year)
    CURRENT_MONTH = datetime.now().month
    QUARTER = 0
    if CURRENT_MONTH <= 3:
        QUARTER = 4
        CURRENT_YEAR_STR = str(int(CURRENT_YEAR_STR) - 1)
    elif CURRENT_MONTH <= 6:
        QUARTER = 1
    elif CURRENT_MONTH <= 9:
        QUARTER = 2
    else:
        QUARTER = 3
    logging.info(f"Creating Spark session for host.docker.internal:27017/idx_lapkeu.idx_lapkeu{CURRENT_YEAR_STR}TW{str(QUARTER)}")
    return (SparkSession.builder
            .appName("IDX Lapkeu Transform")
            .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongodb-driver-3.12.10.jar,/opt/spark/jars/bson-3.12.10.jar,/opt/spark/jars/mongodb-driver-core-3.12.10.jar")
            .config("spark.mongodb.input.uri", "mongodb://host.docker.internal:27017/idx_lapkeu.idx_lapkeu" + CURRENT_YEAR_STR + "TW" + str(QUARTER))
            .config("spark.mongodb.output.uri", "mongodb://host.docker.internal:27017/idx_lapkeu_transformed.idx_lapkeu_" + CURRENT_YEAR_STR + "TW" + str(QUARTER) + "_transformed")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .getOrCreate())

def calculate_sum_if_exists(*columns):
    """Helper function to calculate sum only when at least one value exists."""
    condition = F.lit(False)
    for col in columns:
        condition = condition | col.isNotNull()
    sum_expr = F.lit(0)
    for col in columns:
        sum_expr = sum_expr + F.coalesce(col, F.lit(0))
    return F.when(condition, sum_expr)
