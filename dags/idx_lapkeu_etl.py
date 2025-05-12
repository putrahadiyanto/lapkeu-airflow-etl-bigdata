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
from dags import app_scraper # Import the new scraper module

def create_spark_session():
    """
    Create a spark session with MongoDB connector configuration
    """

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

    logging.info(f"Creating Spark session for host.docker.internal:27017/idx_lapkeu.idx_lapkeu{CURRENT_YEAR_STR}TW{str(QUARTER)}")
    return (SparkSession.builder
            .appName("IDX Lapkeu Transform")
            .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongodb-driver-3.12.10.jar,/opt/spark/jars/bson-3.12.10.jar,/opt/spark/jars/mongodb-driver-core-3.12.10.jar")
            .config("spark.mongodb.input.uri", "mongodb://host.docker.internal:27017/idx_lapkeu.idx_lapkeu" + CURRENT_YEAR_STR + "TW" + str(QUARTER))
            .config("spark.mongodb.output.uri", "mongodb://host.docker.internal:27017/idx_lapkeu_transformed.idx_lapkeu_" + CURRENT_YEAR_STR + "TW" + str(QUARTER) + "_transformed")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .getOrCreate())

def calculate_sum_if_exists(*columns):
    """Helper function to calculate sum only when at least one value exists."""
    # First check if any column is not null
    condition = F.lit(False)
    for col in columns:
        condition = condition | col.isNotNull()
    
    # Calculate the sum with null-safe addition
    sum_expr = F.lit(0)
    for col in columns:
        sum_expr = sum_expr + F.coalesce(col, F.lit(0))
    
    # Return the sum if any column has data, otherwise null
    return F.when(condition, sum_expr)

def transform_data():
    """
    Transform financial data from MongoDB using PySpark.
    This function implements all the transformations from Transformasi_Lapkeu.ipynb
    and saves the transformed data to disk in parquet format.
    The actual loading to MongoDB is handled by the load() function.
    """
    logging.info("Starting transformation process...")
    spark = None  # Initialize spark to None
    try:
        # Initialize Spark session
        spark = create_spark_session()
        
        logging.info(f"Reading data from MongoDB collection specified in Spark session config")
        df = spark.read.format("mongo").load()
        
        row_count = df.count()
        if row_count == 0:
            logging.warning(
                "Input DataFrame from MongoDB is empty (count=0). Skipping transformation."
            )
            return 
        # =====================================
        # 1. BANKS: G1. Banks
        # =====================================
        logging.info("Processing banks data...")
        banks_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G1. Banks"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            
            calculate_sum_if_exists(
                F.col("facts.InterestIncome_CurrentYearDuration.value"),
                F.col("facts.SubtotalShariaIncome_CurrentYearDuration.value")
            ).alias("revenue"),
            
            F.col("facts.ProfitFromOperation_CurrentYearDuration.value").alias("gross_profit"),
            F.col("facts.ProfitFromOperation_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.Cash_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            calculate_sum_if_exists(
                F.col("facts.BorrowingsThirdParties_CurrentYearInstant.value"),
                F.col("facts.BorrowingsRelatedParties_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            calculate_sum_if_exists(
                F.col("facts.SubordinatedLoansThirdParties_CurrentYearInstant.value"),
                F.col("facts.SubordinatedLoansRelatedParties_CurrentYearInstant.value")
            ).alias("long_term_borrowing"),
            
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 2. FINANCING SERVICES: G2. Financing Service
        # =====================================
        logging.info("Processing financing services data...")
        financing_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G2. Financing Service"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            
            calculate_sum_if_exists(
                F.col("facts.IncomeFromMurabahahAndIstishna_CurrentYearDuration.value"),
                F.col("facts.IncomeFromConsumerFinancing_CurrentYearDuration.value"),
                F.col("facts.IncomeFromFinanceLease_CurrentYearDuration.value"),
                F.col("facts.AdministrationIncome_CurrentYearDuration.value"),
                F.col("facts.IncomeFromProvisionsAndCommissions_CurrentYearDuration.value")
            ).alias("revenue"),
            
            calculate_sum_if_exists(
                F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value"),
                F.col("facts.DepreciationOfInvestmentPropertyLeaseAssetsPropertyAndEquipmentForeclosedAssetsAndIjarahAssets_CurrentYearDuration.value")
            ).alias("gross_profit"),
            
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            calculate_sum_if_exists(
                F.col("facts.BorrowingsThirdParties_CurrentYearInstant.value"),
                F.col("facts.CurrentAccountsWithOtherBanksThirdParties_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            calculate_sum_if_exists(
                F.col("facts.BorrowingsRelatedParties_CurrentYearInstant.value"),
                F.col("facts.BondsPayable_CurrentYearInstant.value"),
                F.col("facts.Sukuk_CurrentYearInstant.value")
            ).alias("long_term_borrowing"),
            
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 3. INVESTMENT SERVICE: G3. Investment Service
        # =====================================
        logging.info("Processing investment services data...")
        investment_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G3. Investment Service"
        ).select(
            F.col("ticker").alias("emiten"),
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),

            calculate_sum_if_exists(
                F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value"),
                F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value"),
                F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value")
            ).alias("revenue"),

            F.when(
                (F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value").isNotNull() |
                F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value").isNotNull() |
                F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value").isNotNull()) &
                F.col("facts.GeneralAndAdministrativeExpenses_CurrentYearDuration.value").isNotNull(),
                F.coalesce(F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value"), F.lit(0)) +
                F.coalesce(F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value"), F.lit(0)) +
                F.coalesce(F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value"), F.lit(0)) -
                F.col("facts.GeneralAndAdministrativeExpenses_CurrentYearDuration.value")
            ).alias("gross_profit"),

            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),

            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            F.col("facts.BankLoans_CurrentYearInstant.value").alias("short_term_borrowing"),
            
            F.when(
                F.col("facts.BankLoans_PriorEndYearInstant.value").isNotNull() &
                F.col("facts.BankLoans_CurrentYearInstant.value").isNotNull(),
                F.col("facts.BankLoans_PriorEndYearInstant.value") - F.col("facts.BankLoans_CurrentYearInstant.value")
            ).alias("long_term_borrowing"),
            
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 4. INSURANCE: G4. Insurance
        # =====================================
        logging.info("Processing insurance data...")
        insurance_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G4. Insurance"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.RevenueFromInsurancePremiums_CurrentYearDuration.value").alias("revenue"),
            
            F.when(
                F.col("facts.RevenueFromInsurancePremiums_CurrentYearDuration.value").isNotNull() &
                (F.col("facts.ClaimExpenses_CurrentYearDuration.value").isNotNull() |
                 F.col("facts.ReinsuranceClaims_CurrentYearDuration.value").isNotNull()),
                F.col("facts.RevenueFromInsurancePremiums_CurrentYearDuration.value") -
                F.coalesce(F.col("facts.ClaimExpenses_CurrentYearDuration.value"), F.lit(0)) -
                F.coalesce(F.col("facts.ReinsuranceClaims_CurrentYearDuration.value"), F.lit(0))
            ).alias("gross_profit"),
            
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            calculate_sum_if_exists(
                F.col("facts.ClaimPayables_CurrentYearInstant.value"),
                F.col("facts.ReinsurancePayables_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            F.col("facts.InsuranceLiabilitiesForFuturePolicyBenefits_CurrentYearInstant.value").alias("long_term_borrowing"),
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 5. OTHER SECTORS (non-bank, non-finance, non-insurance)
        # =====================================
        logging.info("Processing general sectors data...")
        non_special_df = df.filter(
            ~F.col("facts.Subsector_CurrentYearInstant.value").isin(["G1. Banks", "G2. Financing Service", "G3. Investment Service", "G4. Insurance"])
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.SalesAndRevenue_CurrentYearDuration.value").alias("revenue"),
            F.col("facts.GrossProfit_CurrentYearDuration.value").alias("gross_profit"),
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            F.coalesce(
                F.col("facts.ShortTermBankLoans_CurrentYearInstant.value"),
                F.col("facts.CurrentMaturitiesOfBankLoans_CurrentYearInstant.value"),
                F.col("facts.OtherCurrentFinancialLiabilities_CurrentYearInstant.value"),
                F.col("facts.ShortTermDerivativeFinancialLiabilities_CurrentYearInstant.value"),
                F.col("facts.CurrentAdvancesFromCustomersThirdParties_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            F.col("facts.LongTermBankLoans_CurrentYearInstant.value").alias("long_term_borrowing"),
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # Combine all DataFrames and Sort
        # =====================================
        logging.info("Combining all sector data...")
        final_df = (
            banks_df
            .unionByName(financing_df)
            .unionByName(investment_df) 
            .unionByName(insurance_df)
            .unionByName(non_special_df)
        )
        final_df = final_df.orderBy("emiten", ascending=True) # Moved sort here
        
        logging.info("Caching and counting final_df for the load step...")
        final_df.cache()
        record_count = final_df.count()

        if record_count == 0:
            logging.warning("Transformed DataFrame is empty after combining all sectors. No data will be saved to Parquet.")
            return # Spark session will be stopped in the finally block

        logging.info(f"Transformation completed successfully with {record_count} records")
        
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

def extract_financial_data():
    """
    Extract task - Runs the Selenium scraper to fetch data from IDX
    and store it in MongoDB.
    """
    logging.info("Starting financial data extraction using Selenium scraper...")
    
    # Define paths and configurations for the scraper
    # These paths are relative to the Airflow worker's environment (Docker container)
    csv_file = "/opt/airflow/dags/emiten.csv" # Ensure emiten.csv is in the dags folder
    download_folder = "/tmp/idx_scraper_downloads" 
    json_folder = None # Set to a path like "/tmp/idx_scraper_json" to save JSONs, else None
    
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
    This now calls the Selenium-based scraper.
    """
    extract_financial_data()

def load():
    """
    Load task - Reads transformed data from disk and writes it to MongoDB collection.
    """
    logging.info("Starting load process...")
    spark = None # Initialize spark to None
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
        
        logging.info("Reading transformed data from disk (/tmp/transformed_data)...")
        
        parquet_path = "/tmp/transformed_data"
        if not os.path.exists(parquet_path) or not os.listdir(parquet_path): # Check if dir exists and is not empty
            logging.warning(f"Parquet data path {parquet_path} does not exist or is empty. Skipping load.")
            return # Spark session will be stopped in the finally block

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
    schedule=None,
    catchup=False,
    tags=['idx', 'financial', 'etl']
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract, # This now calls the scraper
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# Define task dependencies
extract_task >> transform_task >> load_task
