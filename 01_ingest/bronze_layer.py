# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Data Ingestion
# MAGIC
# MAGIC This notebook ingests raw CSV files from ADLS Gen2 into Bronze Delta tables.
# MAGIC
# MAGIC **Data Sources:**
# MAGIC - Transactions (multi-channel: web, mobile, in-store)
# MAGIC - Products (catalog information)
# MAGIC - Customers (customer master data)
# MAGIC - Campaigns (marketing campaign data)

# COMMAND ----------

# MAGIC %run "../00_setup/config_helper"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Transaction Data

# COMMAND ----------

try:
    # Load transaction data
    transactions_df = load_csv_with_schema(TRANSACTION_PATH, transaction_schema)
    
    # Add audit columns
    transactions_df = add_audit_columns(transactions_df)
    
    # Data quality check
    quality_metrics = check_data_quality(transactions_df, "transactions")
    
    # Write to Bronze table
    write_delta_table(
        transactions_df, 
        f"{BRONZE_DB}.transactions", 
        mode="overwrite",
        partition_cols=["transaction_date"]
    )
    
    print("✅ Transaction data ingested successfully to bronze.transactions")
    
except Exception as e:
    print(f"❌ Error ingesting transaction data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Product Data

# COMMAND ----------

try:
    # Load product data
    products_df = load_csv_with_schema(PRODUCT_PATH, product_schema)
    
    # Add audit columns
    products_df = add_audit_columns(products_df)
    
    # Data quality check
    quality_metrics = check_data_quality(products_df, "products")
    
    # Write to Bronze table
    write_delta_table(
        products_df, 
        f"{BRONZE_DB}.products", 
        mode="overwrite"
    )
    
    print("✅ Product data ingested successfully to bronze.products")
    
except Exception as e:
    print(f"❌ Error ingesting product data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customer Data

# COMMAND ----------

try:
    # Load customer data
    customers_df = load_csv_with_schema(CUSTOMER_PATH, customer_schema)
    
    # Add audit columns
    customers_df = add_audit_columns(customers_df)
    
    # Data quality check
    quality_metrics = check_data_quality(customers_df, "customers")
    
    # Write to Bronze table
    write_delta_table(
        customers_df, 
        f"{BRONZE_DB}.customers", 
        mode="overwrite"
    )
    
    print("✅ Customer data ingested successfully to bronze.customers")
    
except Exception as e:
    print(f"❌ Error ingesting customer data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Campaign Data

# COMMAND ----------

try:
    # Load campaign data
    campaigns_df = load_csv_with_schema(CAMPAIGN_PATH, campaign_schema)
    
    # Add audit columns
    campaigns_df = add_audit_columns(campaigns_df)
    
    # Data quality check
    quality_metrics = check_data_quality(campaigns_df, "campaigns")
    
    # Write to Bronze table
    write_delta_table(
        campaigns_df, 
        f"{BRONZE_DB}.campaigns", 
        mode="overwrite"
    )
    
    print("✅ Campaign data ingested successfully to bronze.campaigns")
    
except Exception as e:
    print(f"❌ Error ingesting campaign data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

# Show table counts
print("📊 Bronze Layer Table Counts:")
print(f"Transactions: {spark.table(f'{BRONZE_DB}.transactions').count()}")
print(f"Products: {spark.table(f'{BRONZE_DB}.products').count()}")
print(f"Customers: {spark.table(f'{BRONZE_DB}.customers').count()}")
print(f"Campaigns: {spark.table(f'{BRONZE_DB}.campaigns').count()}")

# COMMAND ----------

# Display sample data
print("📋 Sample Transaction Data:")
spark.table(f"{BRONZE_DB}.transactions").limit(5).display()
