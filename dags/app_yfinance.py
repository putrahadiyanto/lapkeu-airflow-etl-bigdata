import yfinance as yf
import pandas as pd
from datetime import datetime
import uuid
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import os

def load_tickers(csv_file='emiten.csv'):
    try:
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(dag_folder, csv_file)
        print(f"Mencoba membaca file dari: {csv_path}")
        # Adjusted: Use header=0 to skip the header row and use the 'emiten' column
        tickers = pd.read_csv(csv_path, header=0, comment='/')
        ticker_list = tickers['emiten'].dropna().astype(str).tolist()
        print(f"Berhasil memuat {len(ticker_list)} ticker dari {csv_file}")
        return ticker_list
    except Exception as e:
        print(f"Error saat membaca file {csv_file}: {str(e)}")
        return []

def process_stock_data(ticker_symbol):
    try:
        ticker_yf = f"{ticker_symbol}.JK"
        print(f"Mengunduh data terbaru untuk {ticker_symbol}...")
        stock_data = yf.download(ticker_yf, period="1d", progress=False)
        if stock_data.empty:
            print(f"Tidak ada data untuk {ticker_symbol}")
            return []
        json_documents = []
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
                "ticker": ticker_symbol
            }
            json_documents.append(document)
        print(f"Data terbaru {ticker_symbol} berhasil diproses: tanggal {index.date()}")
        return json_documents
    except Exception as e:
        print(f"Error saat mengunduh {ticker_symbol}: {str(e)}")
        return []

def save_to_mongodb(documents, mongo_client, db_name="stock_data", collection_name="Yfinance"):
    try:
        db = mongo_client[db_name]
        collection = db[collection_name]
        if documents:
            result = collection.insert_many(documents)
            return len(result.inserted_ids)
        return 0
    except Exception as e:
        print(f"Error saat menyimpan ke MongoDB: {str(e)}")
        return 0

def connect_mongo():
    try:
        mongo_uri = "mongodb://host.docker.internal:27017/"
        print(f"Mencoba terhubung ke MongoDB dengan URI: {mongo_uri}")
        mongo_client = MongoClient(mongo_uri)
        dbs = mongo_client.list_database_names()
        print(f"Koneksi ke MongoDB berhasil. Database yang tersedia: {dbs}")
        return mongo_client
    except Exception as e:
        print(f"Error saat menghubungkan ke MongoDB: {str(e)}")
        print("Pastikan MongoDB server berjalan di localhost:27017")
        print("Jika Anda menggunakan Docker, pastikan host.docker.internal dapat diakses atau gunakan IP address jaringan Docker")
        return None

def create_dag():
    dag = DAG(
        'stock_data_etl',
        description='ETL for stock data from Yahoo Finance to MongoDB',
        schedule_interval='@daily',
        start_date=datetime(2025, 5, 12),
        catchup=False,
    )
    def load_task():
        return load_tickers(csv_file='emiten.csv')
    def process_and_save_task(**kwargs):
        ti = kwargs['ti']
        ticker_list = ti.xcom_pull(task_ids='load_tickers')
        if not ticker_list:
            print("Tidak ada ticker yang ditemukan. Pastikan file emiten.csv tersedia dan berformat benar.")
            return 0
        print(f"Memproses {len(ticker_list)} ticker: {ticker_list}")
        all_documents = []
        for ticker_symbol in ticker_list:
            documents = process_stock_data(ticker_symbol)
            all_documents.extend(documents)
        if all_documents:
            mongo_client = connect_mongo()
            if mongo_client:
                saved_count = save_to_mongodb(all_documents, mongo_client)
                print(f"Saved {saved_count} records to MongoDB")
                return saved_count
        else:
            print("Tidak ada data yang berhasil diproses untuk disimpan ke MongoDB")
        return 0
    load_tickers_operator = PythonOperator(
        task_id='load_tickers',
        python_callable=load_task,
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
