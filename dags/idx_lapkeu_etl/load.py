import logging
import os
from .spark_utils import create_spark_session
from datetime import datetime

def load():
    """
    Load task - Reads transformed data from disk and writes it to MongoDB collection.
    """
    logging.info("Starting load process...")
    spark = None
    try:
        spark = create_spark_session()
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
        FINAL_COLLECTION = "idx_lapkeu_" + CURRENT_YEAR_STR + "TW" + str(QUARTER) + "_transformed"
        parquet_path = "/tmp/transformed_data"
        if not os.path.exists(parquet_path) or not os.listdir(parquet_path):
            logging.warning(f"Parquet data path {parquet_path} does not exist or is empty. Skipping load.")
            return
        df = spark.read.format("parquet").load(parquet_path)
        if df.count() == 0:
            logging.warning("Transformed DataFrame is empty after reading from disk. No data will be saved to MongoDB.")
            return
        else:
            df = df.orderBy("emiten", ascending=True)
            logging.info(f"Writing data to MongoDB collection {FINAL_COLLECTION}...")
            df.write.format("mongo") \
                .option("uri", "mongodb://host.docker.internal:27017") \
                .option("database", "idx_lapkeu_transformed") \
                .option("collection", FINAL_COLLECTION) \
                .mode("overwrite") \
                .save()
            logging.info(f"Successfully wrote {df.count()} records to MongoDB collection {FINAL_COLLECTION}")
        import shutil
        try:
            shutil.rmtree(parquet_path)
            logging.info("Successfully removed temporary parquet files")
        except Exception as e_cleanup:
            logging.warning(f"Failed to clean up temporary parquet files: {str(e_cleanup)}")
    except Exception as e:
        logging.error(f"An error occurred during the load process: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped in load.")
