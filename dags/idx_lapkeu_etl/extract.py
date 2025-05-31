"""
Extract module for IDX Laporan Keuangan ETL process.

This module handles the extraction of financial data from IDX website
using a Selenium-based scraper and stores it in MongoDB.
"""

import logging
from dags import app_scraper
from dags.idx_lapkeu_etl.spark_utils import get_current_quarter


def extract_financial_data():
    """
    Extract financial data from IDX website and store it in MongoDB.
    
    Uses Selenium-based scraping to fetch financial data from IDX website
    and stores it in the appropriate MongoDB collection.
    """
    logging.info("Starting financial data extraction using Selenium scraper...")
    
    # Define paths and configurations for the scraper
    # These paths are relative to the Airflow worker's environment (Docker container)
    csv_file = "/opt/airflow/dags/emiten.csv"  # Ensure emiten.csv is in the dags folder
    download_folder = "/tmp/idx_scraper_downloads"
    json_folder = None  # Set to a path like "/tmp/idx_scraper_json" to save JSONs, else None
    
    # Get current year and quarter
    CURRENT_YEAR_STR, QUARTER = get_current_quarter()
    
    mongo_db_uri = "mongodb://host.docker.internal:27017/"
    mongo_database_name = "idx_lapkeu"
    # This collection name MUST match the one Spark reads from in create_spark_session's input URI
    mongo_target_collection_name = "idx_lapkeu" + CURRENT_YEAR_STR + "TW" + str(QUARTER)

    # Set to None to use local ChromeDriver (expected in PATH or via CHROMEDRIVER_EXECUTABLE_PATH env var)
    # Or provide Selenium Hub URL e.g., "http://selenium-hub:4444/wd/hub"
    selenium_hub = None

    try:
        app_scraper.run_scraper(
            csv_file_path=csv_file,
            download_dir=download_folder,
            json_output_dir=json_folder,
            mongo_uri=mongo_db_uri,
            mongo_db_name=mongo_database_name,
            mongo_collection_name=mongo_target_collection_name,
            selenium_hub_url=selenium_hub
        )
        logging.info("Financial data extraction completed successfully.")
    except Exception as e:
        logging.error(f"Error during financial data extraction: {str(e)}", exc_info=True)
        raise


def extract():
    """
    Main extract task callable by Airflow.
    This function calls the Selenium-based scraper.
    """
    extract_financial_data()
