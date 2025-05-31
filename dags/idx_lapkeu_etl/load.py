"""
Load module for IDX Laporan Keuangan ETL process.

This module handles the loading of transformed financial data
from parquet files into MongoDB.
"""

import logging
import os
import shutil
from dags.idx_lapkeu_etl.spark_utils import create_spark_session, get_current_quarter


def load():
    """
    Load task - Reads transformed data from disk and writes it to MongoDB collection.
    
    This function reads the transformed data from parquet files and loads it into
    the target MongoDB collection.
    """
    logging.info("Starting load process...")
    spark = None  # Initialize spark to None
    try:
        spark = create_spark_session()
        
        # Get current year and quarter
        CURRENT_YEAR_STR, QUARTER = get_current_quarter()
        
        FINAL_COLLECTION = "idx_lapkeu_" + CURRENT_YEAR_STR + "TW" + str(QUARTER) + "_transformed"
        
        logging.info("Reading transformed data from disk (/tmp/transformed_data)...")
        
        parquet_path = "/tmp/transformed_data"
        if not os.path.exists(parquet_path) or not os.listdir(parquet_path):  # Check if dir exists and is not empty
            logging.warning(f"Parquet data path {parquet_path} does not exist or is empty. Skipping load.")
            return  # Spark session will be stopped in the finally block

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
        
        logging.info(f"Cleaning up temporary parquet files at {parquet_path}...")
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
