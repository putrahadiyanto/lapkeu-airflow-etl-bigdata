import logging
from datetime import datetime
from dags import app_scraper

def extract_financial_data():
    """
    Extract task - Runs the Selenium scraper to fetch data from IDX and store it in MongoDB.
    """
    logging.info("Starting financial data extraction using Selenium scraper...")
    csv_file = "/opt/airflow/dags/emiten.csv"
    download_folder = "/tmp/idx_scraper_downloads"
    json_folder = None
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
    mongo_db_uri = "mongodb://host.docker.internal:27017/"
    mongo_database_name = "idx_lapkeu"
    mongo_target_collection_name = "idx_lapkeu" + CURRENT_YEAR_STR + "TW" + str(QUARTER)
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
    Main extract task callable by Airflow. Calls the Selenium-based scraper.
    """
    extract_financial_data()
