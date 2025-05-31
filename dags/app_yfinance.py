import yfinance as yf
from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import os
import logging
import pymongo
from pymongo import MongoClient

# MongoDB config variables
MONGO_DB_NAME = "YFinance_Data"
MONGO_HOST = "host.docker.internal"
MONGO_PORT = 27017

def get_mongo_client():
    """
    Create and return a MongoDB client
    """
    client = MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
    return client

def load_tickers(csv_file='emiten.csv'):
    try:
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(dag_folder, csv_file)
        logging.info(f"Membaca ticker dari: {csv_path}")
        
        ticker_list = []
        with open(csv_path, 'r') as file:
            # Skip header line
            next(file)
            for line in file:
                ticker = line.strip()
                if ticker:
                    ticker_list.append(ticker)
        
        logging.info(f"Berhasil memuat {len(ticker_list)} ticker dari {csv_file}")
        return ticker_list
    except Exception as e:
        logging.error(f"Error saat membaca file {csv_file}: {str(e)}")
        return []

def process_and_save_task(**kwargs):
    ti = kwargs['ti']
    ticker_list = ti.xcom_pull(task_ids='load_tickers')
    if not ticker_list:
        logging.warning("Tidak ada ticker yang ditemukan. Pastikan file emiten.csv tersedia dan berformat benar.")
        return 0
    logging.info(f"Memproses {len(ticker_list)} ticker: {ticker_list}")
    
    # Create MongoDB client and get database/collection
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]
    
    # Use current date for collection name to store every new fetch separately
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_name = f"stock_data_{current_date}"
    collection = db[collection_name]
    
    total_documents = 0
    for ticker_symbol in ticker_list:
        try:
            ticker_yf = f"{ticker_symbol}.JK"
            logging.info(f"Mengunduh data terbaru untuk {ticker_symbol}...")
            stock_data = yf.download(
                ticker_yf,
                start="2025-05-27",
                end="2025-05-28",
                progress=False
            )
            
            if stock_data.empty:
                logging.warning(f"Tidak ada data untuk {ticker_symbol}")
                continue
            
            logging.info(f"Jumlah baris data yang diunduh untuk {ticker_symbol}: {len(stock_data)}")
            
            # Process and insert each row directly into MongoDB
            documents_for_ticker = []
            for index, row in stock_data.iterrows():
                document = {
                    "_id": str(uuid.uuid4()).replace("-", "")[:24],
                    "Date": index.isoformat(),
                    "Open": float(row['Open']),
                    "High": float(row['High']),
                    "Low": float(row['Low']),
                    "Close": float(row['Close']),
                    "Volume": int(row['Volume']),
                    "Dividends": float(row.get('Dividends', 0)),
                    "Stock Splits": float(row.get('Stock Splits', 0)),
                    "ticker": ticker_symbol,
                    "fetch_timestamp": datetime.now().isoformat()  # Add timestamp for tracking when data was fetched
                }
                documents_for_ticker.append(document)
            
            # Insert batch for better performance
            if documents_for_ticker:
                collection.insert_many(documents_for_ticker)
                total_documents += len(documents_for_ticker)
                
            logging.info(f"Data terbaru {ticker_symbol} berhasil diproses: {len(documents_for_ticker)} dokumen")
            time.sleep(10)  # Add delay to avoid rate limiting
            
        except Exception as e:
            logging.error(f"Error saat mengunduh {ticker_symbol}: {str(e)}")
    
    logging.info(f"Total {total_documents} dokumen berhasil disimpan ke MongoDB collection: {collection_name}")
    
    # Create index on ticker for better query performance
    if total_documents > 0:
        collection.create_index("ticker")
        collection.create_index("Date")
        
    client.close()
    return total_documents

def create_dag():
    dag = DAG(
        'stock_data_etl',
        description='ETL for stock data from Yahoo Finance to MongoDB',
        schedule_interval='@daily',
        start_date=datetime(2025, 5, 12),
        catchup=False,
    )
    load_tickers_operator = PythonOperator(
        task_id='load_tickers',
        python_callable=load_tickers,
        dag=dag
    )
    process_and_save_operator = PythonOperator(
        task_id='process_and_save_data',
        python_callable=process_and_save_task,
        provide_context=True,
        dag=dag
    )
    load_tickers_operator >> process_and_save_operator
    return dag

dag_instance = create_dag()
