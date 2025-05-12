\
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import time
import csv
import os
import zipfile
import json
import datetime
import glob
import shutil
import tempfile
import xml.etree.ElementTree as ET
from pymongo import MongoClient
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables for year and quarter, determined once
CURRENT_YEAR_STR = str(datetime.datetime.now().year)
CURRENT_MONTH = datetime.datetime.now().month
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

def _upload_to_mongodb(data, mongo_uri, db_name, collection_name):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Check for existing data based on ticker, year, and quarter for idempotency
        existing = collection.find_one({
            "ticker": data["ticker"],
            "tahun": data["tahun"],
            "triwulan": data["triwulan"]
        })
        
        if existing:
            result = collection.replace_one({
                "ticker": data["ticker"],
                "tahun": data["tahun"],
                "triwulan": data["triwulan"]
            }, data)
            logging.info(f"Data updated for ticker: {data['ticker']} in {db_name}.{collection_name}")
            return f"Data diperbarui untuk ticker: {data['ticker']}"
        else:
            result = collection.insert_one(data)
            logging.info(f"Data added for ticker: {data['ticker']} to {db_name}.{collection_name} with ID: {result.inserted_id}")
            return f"Data ditambahkan dengan ID: {result.inserted_id}"
    except Exception as e:
        logging.error(f"Error uploading to MongoDB for ticker {data.get('ticker', 'N/A')}: {str(e)}")
        # Re-raise the exception so Airflow can catch it and mark the task as failed
        raise

def _generate_url(ticker, report_year, report_quarter):
    return f"https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan/Laporan%20Keuangan%20Tahun%20{report_year}/TW{report_quarter}/{ticker}/instance.zip"

def _get_latest_downloaded_file_in_dir(download_dir):
    list_of_files = glob.glob(os.path.join(download_dir, '*'))
    if not list_of_files:
        return None
    return max(list_of_files, key=os.path.getctime)

def _extract_instance_xbrl(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                # Ensure case-insensitivity and ignore potential directory entries in zip
                if file_info.filename.lower().endswith('instance.xbrl') and not file_info.is_dir():
                    # Sanitize filename to prevent issues like "instance.xbrl/"
                    actual_filename = os.path.basename(file_info.filename)
                    if actual_filename.lower() == 'instance.xbrl':
                        zip_ref.extract(file_info.filename, extract_to)
                        return os.path.join(extract_to, file_info.filename) # Return full path to extracted file
            logging.warning(f"instance.xbrl not found in {zip_path}")
            return None
    except zipfile.BadZipFile:
        logging.error(f"Bad ZIP file: {zip_path}")
        return None
    except Exception as e:
        logging.error(f"Error extracting ZIP {zip_path}: {e}")
        return None


def _xbrl_to_json(xbrl_path, ticker, report_year, report_quarter):
    try:
        tree = ET.parse(xbrl_path)
        root = tree.getroot()
        
        data = {
            "file_info": {
                "instance_filename": "instance.xbrl",
                "taxonomy_filename": "Taxonomy.xsd", # This is an assumption
                "file_size": os.path.getsize(xbrl_path)
            },
            "ticker": ticker,
            "tahun": report_year,
            "triwulan": f"TW{report_quarter}",
            "facts": {}
        }
        
        for elem in root.findall(".//*"):
            context_ref = elem.attrib.get('contextRef')
            if context_ref:
                tag = elem.tag
                if '}' in tag:
                    tag = tag.split('}', 1)[1]
                
                value = elem.text
                if value is None:
                    value = "" # Store empty string instead of skipping if value is None
                
                key = f"{tag}_{context_ref}"
                data["facts"][key] = {
                    "value": value.strip(), # Strip whitespace from values
                    "context": context_ref,
                    "name": tag
                }
        
        data["processed_date"] = {"$date": datetime.datetime.now().isoformat()}
        return data
    except ET.ParseError as e:
        logging.error(f"XML ParseError for {xbrl_path} (ticker {ticker}): {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Error converting XBRL to JSON for {ticker}: {str(e)}")
        return None

def run_scraper(csv_file_path, download_dir, json_output_dir, mongo_uri, mongo_db_name, mongo_collection_name, selenium_hub_url=None):
    logging.info(f"Starting IDX scraper. Target Year: {CURRENT_YEAR_STR}, Quarter: {QUARTER}")
    logging.info(f"Reading tickers from: {csv_file_path}")
    logging.info(f"Download directory: {download_dir}")
    logging.info(f"JSON output directory (if used): {json_output_dir}")
    logging.info(f"MongoDB: URI={mongo_uri}, DB={mongo_db_name}, Collection={mongo_collection_name}")

    os.makedirs(download_dir, exist_ok=True)
    if json_output_dir:
      os.makedirs(json_output_dir, exist_ok=True)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36") # Added User-Agent
    
    prefs = {"download.default_directory": download_dir, "download.prompt_for_download": False}
    options.add_experimental_option("prefs", prefs)

    driver = None
    try:
        if selenium_hub_url:
            logging.info(f"Using Selenium Hub at {selenium_hub_url}")
            driver = webdriver.Remote(command_executor=selenium_hub_url, options=options)
        else:
            logging.info("Using local ChromeDriver.")
            chrome_driver_exe_path = os.environ.get('CHROMEDRIVER_EXECUTABLE_PATH')
            service_args = {}
            if chrome_driver_exe_path:
                logging.info(f"Using ChromeDriver from env var CHROMEDRIVER_EXECUTABLE_PATH: {chrome_driver_exe_path}")
                if not os.path.exists(chrome_driver_exe_path):
                    logging.error(f"ChromeDriver path from env var does not exist: {chrome_driver_exe_path}")
                    raise FileNotFoundError(f"ChromeDriver not found at {chrome_driver_exe_path}")
                service_args['executable_path'] = chrome_driver_exe_path
            else:
                logging.info("CHROMEDRIVER_EXECUTABLE_PATH not set. Assuming ChromeDriver is in system PATH.")
            
            service = Service(**service_args)
            driver = webdriver.Chrome(service=service, options=options)
        
        logging.info("WebDriver initialized successfully.")

        with open(csv_file_path, 'r', encoding='utf-8-sig') as file: # Use utf-8-sig for potential BOM
            csv_reader = csv.reader(file)
            header = next(csv_reader, None) 
            if header is None:
                logging.warning(f"CSV file {csv_file_path} is empty or has no header.")
                return
            logging.info(f"CSV Header: {header}")


            for row_number, row in enumerate(csv_reader, 1):
                if not row: 
                    logging.warning(f"Skipping empty row {row_number} in CSV.")
                    continue
                ticker = row[0].strip()
                if not ticker:
                    logging.warning(f"Skipping row {row_number} with empty ticker in CSV.")
                    continue

                logging.info(f"Processing ticker: {ticker} (Year: {CURRENT_YEAR_STR}, TW: {QUARTER})")
                url = _generate_url(ticker, CURRENT_YEAR_STR, QUARTER)
                
                current_zip_file_path = None # To store path of successfully downloaded zip
                try:
                    files_before_download = set(os.listdir(download_dir))
                    
                    logging.info(f"Accessing URL: {url}")
                    driver.get(url)
                    
                    # Log page details immediately after get
                    logging.info(f"Current URL after get: {driver.current_url}")
                    logging.info(f"Page title after get: {driver.title}")
                    
                    # Improved download wait logic
                    download_wait_timeout = 30  # seconds
                    poll_interval = 2 # seconds
                    time_elapsed = 0
                    newly_downloaded_zip = None

                    while time_elapsed < download_wait_timeout:
                        time.sleep(poll_interval)
                        time_elapsed += poll_interval
                        files_after_download = set(os.listdir(download_dir))
                        new_files = files_after_download - files_before_download
                        
                        for f_name in new_files:
                            if f_name.lower().endswith('.zip') and not f_name.lower().endswith('.crdownload'):
                                newly_downloaded_zip = os.path.join(download_dir, f_name)
                                logging.info(f"Detected downloaded ZIP: {newly_downloaded_zip}")
                                break
                        if newly_downloaded_zip:
                            break
                        logging.info(f"Waiting for download for {ticker}... ({time_elapsed}s / {download_wait_timeout}s)")

                    if not newly_downloaded_zip:
                        # Fallback: check the latest file if direct detection failed
                        latest_file_in_dir = _get_latest_downloaded_file_in_dir(download_dir)
                        if latest_file_in_dir and latest_file_in_dir.lower().endswith('.zip') and os.path.join(download_dir, os.path.basename(latest_file_in_dir)) not in files_before_download :
                             newly_downloaded_zip = latest_file_in_dir
                             logging.warning(f"Dynamic download detection failed for {ticker}, using latest ZIP in dir: {newly_downloaded_zip}")
                        else:
                            logging.error(f"No new ZIP file downloaded for {ticker} within timeout or by fallback.")
                            # Enhanced debugging for download failure
                            logging.info(f"Page source (first 500 chars) for {ticker} after failed download attempt: {driver.page_source[:500]}")
                            screenshot_path = os.path.join(download_dir, f"{ticker}_debug_screenshot.png")
                            try:
                                driver.save_screenshot(screenshot_path)
                                logging.info(f"Saved debug screenshot to {screenshot_path}")
                            except Exception as ss_err:
                                logging.error(f"Failed to save screenshot for {ticker}: {ss_err}")
                                
                            # Check for HTML files (error pages)
                            html_error_files = [f for f in new_files if f.lower().endswith(('.html', '.htm'))]
                            if html_error_files:
                                logging.warning(f"Potential error page(s) downloaded for {ticker}: {html_error_files}. Please check IDX website or ticker validity for the period.")
                            continue # Skip to next ticker

                    current_zip_file_path = newly_downloaded_zip # Store for cleanup

                    if zipfile.is_zipfile(current_zip_file_path):
                        logging.info(f"Extracting instance.xbrl from {os.path.basename(current_zip_file_path)}...")
                        with tempfile.TemporaryDirectory() as temp_dir_extract:
                            xbrl_path = _extract_instance_xbrl(current_zip_file_path, temp_dir_extract)
                            if xbrl_path:
                                logging.info(f"Converting instance.xbrl to JSON for {ticker}...")
                                json_data = _xbrl_to_json(xbrl_path, ticker, CURRENT_YEAR_STR, QUARTER)
                                if json_data:
                                    if json_output_dir:
                                        json_file_path = os.path.join(json_output_dir, f"{ticker}_{CURRENT_YEAR_STR}_TW{QUARTER}.json")
                                        with open(json_file_path, 'w', encoding='utf-8') as jf:
                                            json.dump(json_data, jf, indent=2, ensure_ascii=False)
                                        logging.info(f"JSON data for {ticker} saved to {json_file_path}")
                                    
                                    _upload_to_mongodb(json_data, mongo_uri, mongo_db_name, mongo_collection_name)
                                else:
                                    logging.error(f"Failed to convert XBRL to JSON for {ticker}. Skipping MongoDB upload.")
                            else:
                                logging.error(f"Could not extract instance.xbrl for {ticker}. ZIP: {os.path.basename(current_zip_file_path)}")
                    else:
                        logging.error(f"Downloaded file {current_zip_file_path} for {ticker} is not a valid ZIP.")
                
                except Exception as e:
                    logging.error(f"Failed to process data for ticker {ticker}: {str(e)}", exc_info=True)
                
                finally:
                    if current_zip_file_path and os.path.exists(current_zip_file_path):
                        try:
                            os.remove(current_zip_file_path)
                            logging.info(f"Cleaned up ZIP file: {current_zip_file_path}")
                        except OSError as e_remove:
                            logging.warning(f"Could not remove ZIP file {current_zip_file_path}: {e_remove}")
                
                time.sleep(5) # Increased pause between tickers for stability
                
    except FileNotFoundError:
        logging.error(f"Ticker CSV file not found at: {csv_file_path}")
        raise
    except Exception as e:
        logging.error(f"An unhandled error occurred in the scraper: {str(e)}", exc_info=True)
        raise
    finally:
        if driver:
            driver.quit()
            logging.info("WebDriver closed.")

    logging.info("Scraping process finished.")

if __name__ == '__main__':
    # Example for local testing (adjust paths and MongoDB details)
    # Ensure emiten.csv is in the same directory or provide full path
    print("Running app_scraper.py locally for testing...")
    
    # Create a dummy emiten.csv for testing if it doesn't exist
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_csv = os.path.join(script_dir, "emiten_test.csv")
    if not os.path.exists(test_csv):
        with open(test_csv, "w", encoding="utf-8") as f:
            f.write("Kode Emiten\\n") # Example header
            f.write("BBCA\\n")       # Example ticker
            f.write("ASII\\n")       # Another example
        print(f"Created dummy emiten_test.csv at {test_csv}")

    # Set CHROMEDRIVER_EXECUTABLE_PATH if not in PATH for local test
    # e.g., os.environ['CHROMEDRIVER_EXECUTABLE_PATH'] = '/usr/local/bin/chromedriver' 
    
    run_scraper(
        csv_file_path=test_csv,
        download_dir=os.path.join(script_dir, "temp_downloads"),
        json_output_dir=os.path.join(script_dir, "temp_json_data"),
        mongo_uri="mongodb://host.docker.internal:27017/", # Use Docker host MongoDB URI
        mongo_db_name="idx_data_test",
        mongo_collection_name=f"financials_{CURRENT_YEAR_STR}_TW{QUARTER}"
    )
    print("Local test run finished.")
