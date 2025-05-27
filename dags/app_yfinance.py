import yfinance as yf
from datetime import datetime
import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# MongoDB config variables
MONGO_DB_NAME = "YFinance_Data"
MONGO_COLLECTION_NAME = datetime.now().strftime("%m_%Y")
MONGO_URI = f"mongodb://host.docker.internal:27017/{MONGO_DB_NAME}.{MONGO_COLLECTION_NAME}"

# Move schema to module level to avoid closure issues
SPARK_SCHEMA = StructType([
    StructField("_id", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Open", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Close", FloatType(), True),
    StructField("Volume", IntegerType(), True),
    StructField("Dividends", FloatType(), True),
    StructField("Stock Splits", FloatType(), True),
    StructField("ticker", StringType(), True)
])

def create_spark_session():
    return (
        SparkSession.builder
        .appName("YFinance ETL")
        .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongodb-driver-3.12.10.jar,/opt/spark/jars/bson-3.12.10.jar,/opt/spark/jars/mongodb-driver-core-3.12.10.jar")
        .config("spark.mongodb.output.uri", MONGO_URI)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

def load_tickers(csv_file='emiten.csv'):
    try:
        spark = create_spark_session()
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        csv_path = os.path.join(dag_folder, csv_file)
        logging.info(f"Membaca ticker dari: {csv_path} dengan Spark")
        df = spark.read.option("header", True).csv(csv_path)
        ticker_list = [row['emiten'] for row in df.select('emiten').collect() if row['emiten']]
        logging.info(f"Berhasil memuat {len(ticker_list)} ticker dari {csv_file}")
        spark.stop()
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
    all_documents = []
    for ticker_symbol in ticker_list:
        try:
            ticker_yf = f"{ticker_symbol}.JK"
            logging.info(f"Mengunduh data terbaru untuk {ticker_symbol}...")
            stock_data = yf.download(ticker_yf, period="1d", progress=False)
            time.sleep(2)  # Add delay to avoid rate limiting
            if stock_data.empty:
                logging.warning(f"Tidak ada data untuk {ticker_symbol}")
                continue
            logging.info(f"Jumlah baris data yang diunduh untuk {ticker_symbol}: {len(stock_data)}")
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
                logging.debug(f"Dokumen yang dibuat untuk {ticker_symbol}: {document}")
                all_documents.append(document)
            logging.info(f"Data terbaru {ticker_symbol} berhasil diproses: tanggal {index.date()}")
        except Exception as e:
            logging.error(f"Error saat mengunduh {ticker_symbol}: {str(e)}")
    logging.info(f"Total dokumen yang akan dimasukkan ke MongoDB: {len(all_documents)}")
    if all_documents:
        spark = create_spark_session()
        logging.info("Membuat DataFrame Spark dari dokumen yang dikumpulkan...")
        df = spark.createDataFrame(all_documents, schema=SPARK_SCHEMA)
        logging.info(f"Jumlah baris dalam DataFrame sebelum disimpan ke MongoDB: {df.count()}")
        logging.info(f"Menyimpan ke MongoDB database: {MONGO_DB_NAME}, collection: {MONGO_COLLECTION_NAME}")
        df.write.format("mongo") \
            .option("database", MONGO_DB_NAME) \
            .option("collection", MONGO_COLLECTION_NAME) \
            .mode("append").save()
        logging.info(f"Saved {df.count()} records to MongoDB via Spark")
        spark.stop()
        return df.count()
    else:
        logging.warning("Tidak ada data yang berhasil diproses untuk disimpan ke MongoDB")
    return 0

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
