# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration & Helper Functions
# MAGIC
# MAGIC This notebook contains all reusable functions and configurations for the ecommerce data pipeline.
# MAGIC
# MAGIC **Functions included:**
# MAGIC - Data loading utilities
# MAGIC - Delta Lake write operations
# MAGIC - Schema definitions
# MAGIC - Data quality checks

# COMMAND ----------

from pyspark.sql import functions as F, types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions

# COMMAND ----------

# Transaction schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("channel", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address", StringType(), True),
    StructField("campaign_id", StringType(), True)
])

# Product schema
product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("cost_price", DoubleType(), False),
    StructField("retail_price", DoubleType(), False),
    StructField("description", StringType(), True)
])

# Customer schema
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("registration_date", DateType(), True)
])

# Campaign schema
campaign_schema = StructType([
    StructField("campaign_id", StringType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("campaign_type", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("budget", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("target_audience", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading Functions

# COMMAND ----------

def load_csv_with_schema(file_path, schema, header=True):
    """
    Load CSV file with predefined schema and error handling
    
    Args:
        file_path (str): Path to CSV file
        schema (StructType): Predefined schema
        header (bool): Whether CSV has header row
    
    Returns:
        DataFrame: Loaded and validated DataFrame
    """
    try:
        logger.info(f"Loading CSV from: {file_path}")
        
        df = (spark.read
              .option("header", header)
              .option("inferSchema", False)
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
              .option("dateFormat", "yyyy-MM-dd")
              .schema(schema)
              .csv(file_path))
        
        logger.info(f"Successfully loaded {df.count()} rows from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading CSV {file_path}: {str(e)}")
        raise

# COMMAND ----------

def add_audit_columns(df):
    """
    Add audit columns to DataFrame for tracking
    
    Args:
        df (DataFrame): Input DataFrame
    
    Returns:
        DataFrame: DataFrame with audit columns added
    """
    return (df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("ingestion_date", F.current_date())
            .withColumn("source_file", F.input_file_name())
            .withColumn("pipeline_run_id", F.lit(spark.sparkContext.applicationId)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Operations

# COMMAND ----------

def write_delta_table(df, table_name, mode="overwrite", partition_cols=None, optimize_write=True):
    """
    Write DataFrame to Delta Lake table with optimizations
    
    Args:
        df (DataFrame): DataFrame to write
        table_name (str): Name of Delta table
        mode (str): Write mode (overwrite, append, merge)
        partition_cols (list): Columns to partition by
        optimize_write (bool): Enable optimize write
    """
    try:
        logger.info(f"Writing to Delta table: {table_name}")
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        
        if optimize_write:
            writer = writer.option("delta.autoOptimize.optimizeWrite", "true")
        
        writer.saveAsTable(table_name)
        
        logger.info(f"Successfully wrote {df.count()} rows to {table_name}")
        
    except Exception as e:
        logger.error(f"Error writing to Delta table {table_name}: {str(e)}")
        raise

# COMMAND ----------

def create_database_if_not_exists(database_name):
    """
    Create database if it doesn't exist
    
    Args:
        database_name (str): Name of database to create
    """
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        logger.info(f"Database {database_name} created or already exists")
    except Exception as e:
        logger.error(f"Error creating database {database_name}: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Functions

# COMMAND ----------

def check_data_quality(df, table_name):
    """
    Perform basic data quality checks
    
    Args:
        df (DataFrame): DataFrame to check
        table_name (str): Name of table for logging
    
    Returns:
        dict: Data quality metrics
    """
    try:
        logger.info(f"Performing data quality checks for {table_name}")
        
        total_rows = df.count()
        total_cols = len(df.columns)
        
        # Check for null values
        null_counts = {}
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
        
        # Check for duplicates (if primary key exists)
        duplicate_count = 0
        if 'transaction_id' in df.columns:
            duplicate_count = df.count() - df.dropDuplicates(['transaction_id']).count()
        elif 'product_id' in df.columns:
            duplicate_count = df.count() - df.dropDuplicates(['product_id']).count()
        elif 'customer_id' in df.columns:
            duplicate_count = df.count() - df.dropDuplicates(['customer_id']).count()
        
        quality_metrics = {
            'table_name': table_name,
            'total_rows': total_rows,
            'total_columns': total_cols,
            'null_counts': null_counts,
            'duplicate_count': duplicate_count,
            'check_timestamp': F.current_timestamp()
        }
        
        logger.info(f"Data quality check completed for {table_name}")
        return quality_metrics
        
    except Exception as e:
        logger.error(f"Error in data quality check for {table_name}: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Variables

# COMMAND ----------

# Database names
BRONZE_DB = "bronze"
SILVER_DB = "silver"
GOLD_DB = "gold"

# Mount point
MOUNT_POINT = "/mnt/ecommerce"

# File paths
TRANSACTION_PATH = f"{MOUNT_POINT}/transactions/*.csv"
PRODUCT_PATH = f"{MOUNT_POINT}/products/*.csv"
CUSTOMER_PATH = f"{MOUNT_POINT}/customers/*.csv"
CAMPAIGN_PATH = f"{MOUNT_POINT}/campaigns/*.csv"

# Create databases
create_database_if_not_exists(BRONZE_DB)
create_database_if_not_exists(SILVER_DB)
create_database_if_not_exists(GOLD_DB)

print("✅ Configuration and helper functions loaded successfully!")
print(f"✅ Databases created: {BRONZE_DB}, {SILVER_DB}, {GOLD_DB}")
print(f"✅ Mount point configured: {MOUNT_POINT}")
