# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Customer Insights & KPIs
# MAGIC
# MAGIC This notebook creates the Gold layer with curated business insights including:
# MAGIC - Customer average order value (AOV)
# MAGIC - Top products and categories by revenue
# MAGIC - Campaign performance analysis
# MAGIC - Customer segmentation insights
# MAGIC - Multi-channel behavior analysis

# COMMAND ----------

# MAGIC %run "../00_setup/config_helper"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Silver Layer Data

# COMMAND ----------

# Load silver layer tables
enriched_sales = spark.table(f"{SILVER_DB}.enriched_sales")
channel_performance = spark.table(f"{SILVER_DB}.channel_performance")

print("✅ Silver layer tables loaded successfully")
print(f"📊 Enriched sales records: {enriched_sales.count()}")
print(f"📊 Channel performance records: {channel_performance.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculate Average Order Value (AOV) by Customer

# COMMAND ----------

try:
    # Calculate AOV for each customer
    aov = (enriched_sales
        .groupBy("customer_id", "customer_name", "customer_segment", "city", "state")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.count("transaction_id").alias("total_orders"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("quantity").alias("total_items_purchased"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("channel").alias("channels_used"),
            F.max("transaction_date").alias("last_purchase_date"),
            F.min("transaction_date").alias("first_purchase_date")
        )
        .withColumn("customer_lifetime_days", 
                   F.datediff(F.col("last_purchase_date"), F.col("first_purchase_date")))
        .withColumn("avg_items_per_order", 
                   F.round(F.col("total_items_purchased") / F.col("total_orders"), 2))
        .withColumn("purchase_frequency", 
                   F.when(F.col("customer_lifetime_days") > 0, 
                         F.round(F.col("total_orders") / F.col("customer_lifetime_days") * 30, 2))
                   .otherwise(0))
    )
    
    # Add customer value tiers
    aov = (aov
        .withColumn("customer_tier",
            F.when(F.col("avg_order_value") >= 100, "Premium")
            .when(F.col("avg_order_value") >= 50, "Gold")
            .when(F.col("avg_order_value") >= 25, "Silver")
            .otherwise("Bronze"))
    )
    
    print("✅ Customer AOV calculated successfully")
    print(f"📊 Total unique customers: {aov.count()}")
    
    # Show sample AOV data
    print("\n📋 Sample AOV Data:")
    aov.orderBy(F.desc("avg_order_value")).limit(10).display()
    
except Exception as e:
    print(f"❌ Error calculating AOV: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Top Products and Categories

# COMMAND ----------

try:
    # Top products by revenue
    top_products = (enriched_sales
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.count("transaction_id").alias("total_transactions"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.sum("profit_margin").alias("total_profit")
        )
        .withColumn("profit_margin_pct", 
                   F.round(F.col("total_profit") / F.col("total_revenue") * 100, 2))
        .withColumn("revenue_per_customer", 
                   F.round(F.col("total_revenue") / F.col("unique_customers"), 2))
        .orderBy(F.desc("total_revenue"))
    )
    
    # Top categories by revenue
    top_categories = (enriched_sales
        .groupBy("category")
        .agg(
            F.sum("total_amount").alias("category_revenue"),
            F.sum("quantity").alias("category_quantity"),
            F.countDistinct("product_id").alias("unique_products"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_amount").alias("avg_order_value")
        )
        .withColumn("revenue_per_product", 
                   F.round(F.col("category_revenue") / F.col("unique_products"), 2))
        .orderBy(F.desc("category_revenue"))
    )
    
    print("✅ Product and category analysis completed")
    print(f"📊 Total products analyzed: {top_products.count()}")
    print(f"📊 Total categories analyzed: {top_categories.count()}")
    
    # Show top 10 products
    print("\n📋 Top 10 Products by Revenue:")
    top_products.limit(10).display()
    
    # Show category performance
    print("\n📋 Category Performance:")
    top_categories.display()
    
except Exception as e:
    print(f"❌ Error in product analysis: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Campaign Performance Analysis

# COMMAND ----------

try:
    # Campaign impact analysis
    campaign_impact = (enriched_sales
        .filter(F.col("campaign_id").isNotNull())
        .groupBy("campaign_id", "campaign_name", "campaign_type", "channel")
        .agg(
            F.sum("total_amount").alias("campaign_revenue"),
            F.count("transaction_id").alias("campaign_transactions"),
            F.countDistinct("customer_id").alias("campaign_customers"),
            F.avg("total_amount").alias("avg_campaign_order_value"),
            F.sum("profit_margin").alias("campaign_profit")
        )
        .withColumn("revenue_per_customer", 
                   F.round(F.col("campaign_revenue") / F.col("campaign_customers"), 2))
        .withColumn("profit_margin_pct", 
                   F.round(F.col("campaign_profit") / F.col("campaign_revenue") * 100, 2))
        .orderBy(F.desc("campaign_revenue"))
    )
    
    # Compare campaign vs non-campaign performance
    campaign_comparison = (enriched_sales
        .withColumn("has_campaign", F.when(F.col("campaign_id").isNotNull(), "With Campaign").otherwise("Without Campaign"))
        .groupBy("has_campaign")
        .agg(
            F.avg("total_amount").alias("avg_order_value"),
            F.sum("total_amount").alias("total_revenue"),
            F.count("transaction_id").alias("total_transactions")
        )
    )
    
    print("✅ Campaign analysis completed")
    print(f"📊 Active campaigns analyzed: {campaign_impact.count()}")
    
    # Show campaign performance
    print("\n📋 Campaign Performance:")
    campaign_impact.display()
    
    # Show campaign vs non-campaign comparison
    print("\n📋 Campaign vs Non-Campaign Performance:")
    campaign_comparison.display()
    
except Exception as e:
    print(f"❌ Error in campaign analysis: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Layer Customer Insights

# COMMAND ----------

try:
    # Join all insights together for comprehensive customer view
    gold_insights = (aov.alias("a")
        .join(
            # Get customer's top category
            enriched_sales.groupBy("customer_id")
            .agg(F.first("category").alias("top_category"))
            .alias("tc"), 
            "customer_id", "left"
        )
        .join(
            # Get customer's preferred channel
            enriched_sales.groupBy("customer_id")
            .agg(F.first("channel").alias("preferred_channel"))
            .alias("pc"), 
            "customer_id", "left"
        )
        .join(
            # Get campaign engagement
            enriched_sales.groupBy("customer_id")
            .agg(F.countDistinct("campaign_id").alias("campaigns_engaged"))
            .alias("ce"), 
            "customer_id", "left"
        )
        .select(
            # Customer identifiers
            F.col("a.customer_id"),
            F.col("a.customer_name"),
            F.col("a.customer_segment"),
            F.col("a.city"),
            F.col("a.state"),
            
            # Purchase behavior metrics
            F.col("a.total_spent"),
            F.col("a.total_orders"),
            F.col("a.avg_order_value"),
            F.col("a.total_items_purchased"),
            F.col("a.avg_items_per_order"),
            F.col("a.purchase_frequency"),
            
            # Customer preferences
            F.col("tc.top_category"),
            F.col("pc.preferred_channel"),
            F.col("a.channels_used"),
            
            # Engagement metrics
            F.col("ce.campaigns_engaged"),
            F.col("a.unique_products"),
            
            # Dates and tenure
            F.col("a.first_purchase_date"),
            F.col("a.last_purchase_date"),
            F.col("a.customer_lifetime_days"),
            
            # Customer classification
            F.col("a.customer_tier"),
            
            # Derived insights
            F.when(F.col("a.customer_lifetime_days") <= 30, "New")
            .when(F.col("a.customer_lifetime_days") <= 90, "Recent")
            .when(F.col("a.customer_lifetime_days") <= 365, "Active")
            .otherwise("Loyal").alias("customer_lifecycle_stage"),
            
            # Recency, Frequency, Monetary (RFM) components
            F.datediff(F.current_date(), F.col("a.last_purchase_date")).alias("days_since_last_purchase"),
            F.col("a.total_orders").alias("frequency_score"),
            F.col("a.total_spent").alias("monetary_score")
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
    )
    
    print("✅ Gold layer customer insights created successfully")
    print(f"📊 Total customer insights: {gold_insights.count()}")
    
    # Show sample data
    print("\n📋 Sample Customer Insights:")
    gold_insights.limit(10).display()
    
except Exception as e:
    print(f"❌ Error creating gold insights: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold Layer Managed Delta Table

# COMMAND ----------

try:
    # Write the gold insights to managed Delta table
    write_delta_table(
        gold_insights,
        f"{GOLD_DB}.customer_insights",
        mode="overwrite",
        optimize_write=True
    )
    
    print("✅ Gold layer customer insights written successfully")
    print(f"📊 Table location: {GOLD_DB}.customer_insights")
    print("📋 This is a MANAGED Delta table - Databricks handles storage location automatically")
    
    # Verify the table was created
    table_info = spark.sql(f"DESCRIBE EXTENDED {GOLD_DB}.customer_insights")
    print("\n📋 Table Information:")
    table_info.filter(F.col("col_name").isin(["Location", "Provider", "Table Type"])).display()
    
except Exception as e:
    print(f"❌ Error writing gold insights table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Executive Summary Views

# COMMAND ----------

try:
    # Customer tier summary
    customer_tier_summary = (gold_insights
        .groupBy("customer_tier")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.avg("avg_order_value").alias("avg_aov"),
            F.sum("total_spent").alias("total_revenue"),
            F.avg("total_orders").alias("avg_orders_per_customer")
        )
        .withColumn("revenue_percentage", 
                   F.round(F.col("total_revenue") / F.sum("total_revenue").over() * 100, 2))
        .orderBy(F.desc("total_revenue"))
    )
    
    # Channel preference summary
    channel_preference_summary = (gold_insights
        .groupBy("preferred_channel")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.avg("avg_order_value").alias("avg_aov"),
            F.sum("total_spent").alias("total_revenue")
        )
        .orderBy(F.desc("customer_count"))
    )
    
    # Lifecycle stage summary
    lifecycle_summary = (gold_insights
        .groupBy("customer_lifecycle_stage")
        .agg(
            F.count("customer_id").alias("customer_count"),
            F.avg("avg_order_value").alias("avg_aov"),
            F.avg("days_since_last_purchase").alias("avg_recency")
        )
        .orderBy(F.desc("customer_count"))
    )
    
    print("✅ Executive summaries created successfully")
    
    # Display summaries
    print("\n📊 Customer Tier Summary:")
    customer_tier_summary.display()
    
    print("\n📊 Channel Preference Summary:")
    channel_preference_summary.display()
    
    print("\n📊 Customer Lifecycle Summary:")
    lifecycle_summary.display()
    
except Exception as e:
    print(f"❌ Error creating executive summaries: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

try:
    # Perform data quality checks on the gold table
    quality_metrics = check_data_quality(gold_insights, "customer_insights")
    
    # Additional business logic validations
    validation_results = {
        "total_customers": gold_insights.count(),
        "customers_with_purchases": gold_insights.filter(F.col("total_orders") > 0).count(),
        "customers_with_valid_aov": gold_insights.filter(F.col("avg_order_value") > 0).count(),
        "customers_with_tier": gold_insights.filter(F.col("customer_tier").isNotNull()).count(),
        "avg_customer_lifetime_days": gold_insights.agg(F.avg("customer_lifetime_days")).collect()[0][0]
    }
    
    print("✅ Data quality validation completed")
    print(f"📊 Validation Results:")
    for key, value in validation_results.items():
        print(f"   {key}: {value}")
    
    # Check for any data quality issues
    if validation_results["customers_with_purchases"] != validation_results["total_customers"]:
        print("⚠️  Warning: Some customers have no purchases")
    
    if validation_results["customers_with_valid_aov"] != validation_results["total_customers"]:
        print("⚠️  Warning: Some customers have invalid AOV")
    
    print("\n✅ Gold layer customer insights table is ready for consumption!")
    
except Exception as e:
    print(f"❌ Error in data quality validation: {str(e)}")
    raise

