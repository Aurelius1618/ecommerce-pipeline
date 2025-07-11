# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Data Transformation
# MAGIC
# MAGIC This notebook transforms Bronze layer data into Silver layer tables with:
# MAGIC - Data cleansing and validation
# MAGIC - Joins between different entities
# MAGIC - Business logic application
# MAGIC - Multi-channel data consolidation

# COMMAND ----------

# MAGIC %run "../00_setup/config_helper"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Tables

# COMMAND ----------

# Load bronze tables
transactions_bronze = spark.table(f"{BRONZE_DB}.transactions")
products_bronze = spark.table(f"{BRONZE_DB}.products")
customers_bronze = spark.table(f"{BRONZE_DB}.customers")
campaigns_bronze = spark.table(f"{BRONZE_DB}.campaigns")

print("✅ Bronze tables loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Enriched Sales Data

# COMMAND ----------

# Join transactions with products and customers
enriched_sales = (transactions_bronze.alias("t")
    .join(products_bronze.alias("p"), "product_id", "left")
    .join(customers_bronze.alias("c"), "customer_id", "left")
    .join(campaigns_bronze.alias("cam"), "campaign_id", "left")
    .select(
        # Transaction details
        F.col("t.transaction_id"),
        F.col("t.customer_id"),
        F.col("t.product_id"),
        F.col("t.quantity"),
        F.col("t.unit_price"),
        F.col("t.total_amount"),
        F.col("t.transaction_date"),
        F.col("t.channel"),
        F.col("t.payment_method"),
        F.col("t.campaign_id"),
        
        # Product details
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.subcategory"),
        F.col("p.brand"),
        F.col("p.cost_price"),
        F.col("p.retail_price"),
        
        # Customer details
        F.col("c.customer_name"),
        F.col("c.customer_segment"),
        F.col("c.city"),
        F.col("c.state"),
        
        # Campaign details
        F.col("cam.campaign_name"),
        F.col("cam.campaign_type"),
        
        # Calculated fields
        (F.col("t.total_amount") - (F.col("p.cost_price") * F.col("t.quantity"))).alias("profit_margin"),
        (F.col("t.unit_price") / F.col("p.retail_price")).alias("discount_rate"),
        
        # Audit columns
        F.col("t.ingestion_timestamp"),
        F.col("t.source_file")
    )
    .withColumn("year", F.year(F.col("transaction_date")))
    .withColumn("month", F.month(F.col("transaction_date")))
    .withColumn("quarter", F.quarter(F.col("transaction_date")))
)

print("✅ Enriched sales data created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Channel Performance Data

# COMMAND ----------

# Channel performance metrics
channel_performance = (enriched_sales
    .groupBy("channel", "year", "month")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("total_amount").alias("avg_order_value"),
        F.count("transaction_id").alias("total_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("profit_margin").alias("total_profit"),
        F.avg("discount_rate").alias("avg_discount_rate")
    )
    .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("unique_customers"))
    .withColumn("profit_margin_pct", F.col("total_profit") / F.col("total_revenue") * 100)
)

print("✅ Channel performance data created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Tables

# COMMAND ----------

try:
    # Write enriched sales to Silver
    write_delta_table(
        enriched_sales,
        f"{SILVER_DB}.enriched_sales",
        mode="overwrite",
        partition_cols=["year", "month"]
    )
    
    print("✅ Enriched sales data written to silver.enriched_sales")
    
    # Write channel performance to Silver
    write_delta_table(
        channel_performance,
        f"{SILVER_DB}.channel_performance",
        mode="overwrite"
    )
    
    print("✅ Channel performance data written to silver.channel_performance")
    
except Exception as e:
    print(f"❌ Error writing Silver tables: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Tables

# COMMAND ----------

print("📊 Silver Layer Table Counts:")
print(f"Enriched Sales: {spark.table(f'{SILVER_DB}.enriched_sales').count()}")
print(f"Channel Performance: {spark.table(f'{SILVER_DB}.channel_performance').count()}")

# Display sample data
print("📋 Sample Enriched Sales Data:")
spark.table(f"{SILVER_DB}.enriched_sales").limit(5).display()
