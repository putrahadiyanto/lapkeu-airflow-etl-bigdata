import logging
from .spark_utils import create_spark_session, calculate_sum_if_exists
from pyspark.sql import functions as F

def transform_data():
    """
    Transform financial data from MongoDB using PySpark.
    Implements all the transformations from Transformasi_Lapkeu.ipynb
    and saves the transformed data to disk in parquet format.
    """
    logging.info("Starting transformation process...")
    spark = None
    try:
        spark = create_spark_session()
        df = spark.read.format("mongo").load()
        row_count = df.count()
        if row_count == 0:
            logging.warning("Input DataFrame from MongoDB is empty (count=0). Skipping transformation.")
            return
        # ...existing code for banks_df, financing_df, investment_df, insurance_df, non_special_df, final_df...
        # (Copy the sector processing and combining logic from the original transform_data)
        # ...existing code...
        final_df.write.format("parquet") \
            .mode("overwrite") \
            .save("/tmp/transformed_data")
        logging.info(f"Successfully saved transformed data to disk for the load step")
    except Exception as e:
        logging.error(f"An error occurred during data transformation: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logging.info("Spark session stopped in transform_data.")
