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

def create_spark_session():
    """
    Create a spark session with MongoDB connector configuration
    """
    return (SparkSession.builder
            .appName("IDX Lapkeu Transform")
            .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,/opt/spark/jars/mongodb-driver-3.12.10.jar,/opt/spark/jars/bson-3.12.10.jar,/opt/spark/jars/mongodb-driver-core-3.12.10.jar")
            .config("spark.mongodb.input.uri", "mongodb://host.docker.internal:27017/bigdatatugas.2024")
            .config("spark.mongodb.output.uri", "mongodb://host.docker.internal:27017/bigdatatugas.transformed")
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
    """
    logging.info("Starting transformation process...")
    
    # Initialize Spark session
    spark = create_spark_session()
    
    # Read data from MongoDB
    logging.info("Reading data from MongoDB...")
    df = spark.read.format("mongo").load()
    
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
        
        # Revenue components
        calculate_sum_if_exists(
            F.col("facts.InterestIncome_CurrentYearDuration.value"),
            F.col("facts.SubtotalShariaIncome_CurrentYearDuration.value")
        ).alias("revenue"),
        
        F.col("facts.ProfitFromOperation_CurrentYearDuration.value").alias("gross_profit"),
        F.col("facts.ProfitFromOperation_CurrentYearDuration.value").alias("operating_profit"),
        F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
        F.col("facts.Cash_CurrentYearInstant.value").alias("cash"),
        F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
        
        # Short-term borrowing
        calculate_sum_if_exists(
            F.col("facts.BorrowingsThirdParties_CurrentYearInstant.value"),
            F.col("facts.BorrowingsRelatedParties_CurrentYearInstant.value")
        ).alias("short_term_borrowing"),
        
        # Long-term borrowing
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
        
        # Revenue - calculate only if at least one component exists
        calculate_sum_if_exists(
            F.col("facts.IncomeFromMurabahahAndIstishna_CurrentYearDuration.value"),
            F.col("facts.IncomeFromConsumerFinancing_CurrentYearDuration.value"),
            F.col("facts.IncomeFromFinanceLease_CurrentYearDuration.value"),
            F.col("facts.AdministrationIncome_CurrentYearDuration.value"),
            F.col("facts.IncomeFromProvisionsAndCommissions_CurrentYearDuration.value")
        ).alias("revenue"),
        
        # Gross profit
        calculate_sum_if_exists(
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value"),
            F.col("facts.DepreciationOfInvestmentPropertyLeaseAssetsPropertyAndEquipmentForeclosedAssetsAndIjarahAssets_CurrentYearDuration.value")
        ).alias("gross_profit"),
        
        F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
        F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
        F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
        F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
        
        # Short-term borrowing
        calculate_sum_if_exists(
            F.col("facts.BorrowingsThirdParties_CurrentYearInstant.value"),
            F.col("facts.CurrentAccountsWithOtherBanksThirdParties_CurrentYearInstant.value")
        ).alias("short_term_borrowing"),
        
        # Long-term borrowing
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

        # Income Statement
        calculate_sum_if_exists(
            F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value"),
            F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value"),
            F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value")
        ).alias("revenue"),

        # Gross profit calculation with conditional check
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

        # Balance Sheet
        F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
        F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
        F.col("facts.BankLoans_CurrentYearInstant.value").alias("short_term_borrowing"),
        
        # Long-term borrowing: only calculate if both components are available
        F.when(
            F.col("facts.BankLoans_PriorEndYearInstant.value").isNotNull() &
            F.col("facts.BankLoans_CurrentYearInstant.value").isNotNull(),
            F.col("facts.BankLoans_PriorEndYearInstant.value") - F.col("facts.BankLoans_CurrentYearInstant.value")
        ).alias("long_term_borrowing"),
        
        F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
        F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),

        # Cash Flow
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
        
        # Gross profit calculation with conditional check
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
        
        # Short-term borrowing
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
        
        # Use coalesce to find first non-null value for short-term borrowing
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
    
    # Sort by emiten name descending
    final_df = final_df.orderBy("emiten", ascending=False)
    
    # Write the transformed data back to MongoDB
    logging.info("Writing transformed data to MongoDB...")
    STOCK_COLLECTION_OUTPUT = "2024_transformed"
    try:
        final_df.write.format("mongo") \
            .option("uri", "mongodb://host.docker.internal:27017") \
            .option("database", "bigdatatugas") \
            .option("collection", STOCK_COLLECTION_OUTPUT) \
            .mode("overwrite") \
            .save()
        logging.info(f"Successfully wrote {final_df.count()} records to MongoDB collection {STOCK_COLLECTION_OUTPUT}")
    except Exception as e:
        logging.error(f"An error occurred while writing to MongoDB: {str(e)}")
        raise

def extract():
    """
    Extract task - does nothing as per requirements.
    In a complete ETL pipeline, this would fetch data from the source.
    """
    logging.info("Extract step - No action needed as per requirements")
    pass

def load():
    """
    Load task - does nothing as per requirements.
    In a complete ETL pipeline, this would load data to the target system.
    """
    logging.info("Load step - No action needed as per requirements")
    pass

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
    python_callable=extract,
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
