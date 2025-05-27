"""
IDX Laporan Keuangan ETL DAG

This DAG implements the transformation of financial reporting data from IDX.
It reads data from MongoDB, transforms it using PySpark, and writes it back to MongoDB.
This implementation is based on the code from 'Transformasi_Lapkeu.ipynb'.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import logging
from idx_lapkeu_etl.spark_utils import create_spark_session, calculate_sum_if_exists
from idx_lapkeu_etl.transform import transform_data
from idx_lapkeu_etl.extract import extract, extract_financial_data
from idx_lapkeu_etl.load import load

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 10),
}

# Define the DAG
dag = DAG(
    'idx_lapkeu_etl',
    default_args=default_args,
    description='IDX Laporan Keuangan ETL process',
    schedule_interval='0 0 1 2,5,8,11 *',
    start_date=datetime(2023, 2, 1),
    catchup=False,
    tags=['idx', 'financial', 'etl']
)

extract_task = PythonOperator(
    task_id='scrapping_data_idx',
    python_callable=extract, # This now calls the scraper
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data_using_pyspark',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_into_mongodb',
    python_callable=load,
    dag=dag
)

# Define task dependencies
extract_task >> transform_task >> load_task
