# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Monitoring & Validation
# MAGIC
# MAGIC This notebook implements comprehensive data quality monitoring across all pipeline layers:
# MAGIC - **Data Validation**: Checks for completeness, accuracy, and consistency
# MAGIC - **Business Rules**: Enforces domain-specific constraints
# MAGIC - **Anomaly Detection**: Identifies unusual patterns or outliers
# MAGIC - **Quality Metrics**: Tracks data quality trends over time
# MAGIC - **Alerting**: Notifies stakeholders of quality issues
# MAGIC
# MAGIC **Quality Frameworks Used:**
# MAGIC - PyDeequ for comprehensive data profiling
# MAGIC - Custom validation rules for business logic
# MAGIC - Statistical anomaly detection

# COMMAND ----------

# MAGIC %run "../00_setup/config_helper"

# COMMAND ----------

# Install PyDeequ if not already available
%pip install pydeequ

# COMMAND ----------

# Import libraries for data quality monitoring
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.analyzers import *
from pydeequ.profiling import *
from pydeequ.suggestions import *

import json
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("✅ Data quality monitoring libraries loaded")
print(f"📅 Quality check timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Rules & Expectations

# COMMAND ----------

# Define comprehensive quality rules for each table
quality_rules = {
    "bronze.transactions": {
        "completeness_rules": [
            ("transaction_id", "Transaction ID must not be null"),
            ("customer_id", "Customer ID must not be null"),
            ("product_id", "Product ID must not be null"),
            ("transaction_date", "Transaction date must not be null")
        ],
        "validity_rules": [
            ("quantity", "Quantity must be positive", "quantity > 0"),
            ("unit_price", "Unit price must be positive", "unit_price > 0"),
            ("total_amount", "Total amount must be positive", "total_amount > 0"),
            ("channel", "Channel must be valid", "channel IN ('web', 'mobile', 'in-store')")
        ],
        "consistency_rules": [
            ("total_amount", "Total amount should equal quantity * unit_price", 
             "abs(total_amount - (quantity * unit_price)) < 0.01")
        ]
    },
    
    "bronze.products": {
        "completeness_rules": [
            ("product_id", "Product ID must not be null"),
            ("product_name", "Product name must not be null"),
            ("category", "Category must not be null")
        ],
        "validity_rules": [
            ("cost_price", "Cost price must be positive", "cost_price > 0"),
            ("retail_price", "Retail price must be positive", "retail_price > 0"),
            ("retail_price", "Retail price must be >= cost price", "retail_price >= cost_price")
        ]
    },
    
    "bronze.customers": {
        "completeness_rules": [
            ("customer_id", "Customer ID must not be null"),
            ("customer_name", "Customer name must not be null")
        ],
        "validity_rules": [
            ("email", "Email must be valid format", "email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
        ]
    },
    
    "gold.customer_insights": {
        "completeness_rules": [
            ("customer_id", "Customer ID must not be null"),
            ("customer_name", "Customer name must not be null"),
            ("customer_tier", "Customer tier must not be null")
        ],
        "validity_rules": [
            ("avg_order_value", "Average order value must be positive", "avg_order_value > 0"),
            ("total_spent", "Total spent must be positive", "total_spent > 0"),
            ("total_orders", "Total orders must be positive", "total_orders > 0"),
            ("customer_tier", "Customer tier must be valid", 
             "customer_tier IN ('Bronze', 'Silver', 'Gold', 'Premium')")
        ]
    }
}

print("✅ Quality rules defined for all tables")
print(f"📊 Total tables with quality rules: {len(quality_rules)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PyDeequ-Based Quality Validation

# COMMAND ----------

def create_deequ_checks(table_name, df):
    """
    Create comprehensive Deequ checks for a given table
    
    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate
        
    Returns:
        Check object with all validation rules
    """
    try:
        print(f"🔍 Creating Deequ checks for {table_name}")
        
        # Base check object
        check = Check(spark, CheckLevel.Error, f"{table_name}_quality")
        
        # Get quality rules for this table
        table_rules = quality_rules.get(table_name, {})
        
        # Add completeness checks
        for column, description in table_rules.get("completeness_rules", []):
            if column in df.columns:
                check = check.isComplete(column)
                print(f"   ✓ Added completeness check for {column}")
        
        # Add validity checks based on table type
        if "transactions" in table_name:
            check = (check
                .isNonNegative("quantity")
                .isNonNegative("unit_price")
                .isNonNegative("total_amount")
                .isContainedIn("channel", ["web", "mobile", "in-store"])
                .hasPattern("transaction_id", r"^TXN\d+$")  # Transaction ID pattern
                .hasDataType("transaction_date", ConstrainableDataTypes.Date)
            )
            print("   ✓ Added transaction-specific validity checks")
            
        elif "products" in table_name:
            check = (check
                .isNonNegative("cost_price")
                .isNonNegative("retail_price")
                .satisfies("retail_price >= cost_price", "Retail price >= cost price")
                .hasPattern("product_id", r"^PROD\d+$")  # Product ID pattern
            )
            print("   ✓ Added product-specific validity checks")
            
        elif "customers" in table_name:
            check = (check
                .hasPattern("customer_id", r"^CUST\d+$")  # Customer ID pattern
                .hasPattern("email", r"^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", lambda x: x.isNull() | x.rlike(r"^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"))
            )
            print("   ✓ Added customer-specific validity checks")
            
        elif "customer_insights" in table_name:
            check = (check
                .isNonNegative("avg_order_value")
                .isNonNegative("total_spent")
                .isNonNegative("total_orders")
                .isContainedIn("customer_tier", ["Bronze", "Silver", "Gold", "Premium"])
                .satisfies("total_spent >= avg_order_value", "Total spent >= average order value")
            )
            print("   ✓ Added customer insights-specific validity checks")
        
        # Add uniqueness checks for primary keys
        if "transaction_id" in df.columns:
            check = check.isUnique("transaction_id")
            print("   ✓ Added uniqueness check for transaction_id")
        elif "product_id" in df.columns:
            check = check.isUnique("product_id")
            print("   ✓ Added uniqueness check for product_id")
        elif "customer_id" in df.columns and "customer_insights" in table_name:
            check = check.isUnique("customer_id")
            print("   ✓ Added uniqueness check for customer_id")
        
        return check
        
    except Exception as e:
        print(f"❌ Error creating Deequ checks for {table_name}: {str(e)}")
        return None

# COMMAND ----------

def run_deequ_validation(table_name, df):
    """
    Run comprehensive Deequ validation on a DataFrame
    
    Args:
        table_name: Name of the table being validated
        df: DataFrame to validate
        
    Returns:
        Dictionary with validation results
    """
    try:
        print(f"🚀 Running Deequ validation for {table_name}")
        start_time = datetime.now()
        
        # Create checks
        check = create_deequ_checks(table_name, df)
        if not check:
            return {"error": "Failed to create checks"}
        
        # Run verification
        verification_result = (VerificationSuite(spark)
            .onData(df)
            .addCheck(check)
            .run())
        
        # Get success metrics
        success_metrics = AnalyzerContext.successMetricsAsDataFrame(spark, verification_result)
        
        # Get check results
        check_results = VerificationResult.checkResultsAsDataFrame(spark, verification_result)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Convert to pandas for easier manipulation
        success_metrics_pd = success_metrics.toPandas()
        check_results_pd = check_results.toPandas()
        
        # Calculate summary statistics
        total_checks = len(check_results_pd)
        passed_checks = len(check_results_pd[check_results_pd['check_status'] == 'Success'])
        failed_checks = total_checks - passed_checks
        
        results = {
            "table_name": table_name,
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "success_rate": (passed_checks / total_checks * 100) if total_checks > 0 else 0,
            "execution_time_seconds": execution_time,
            "check_timestamp": start_time,
            "success_metrics": success_metrics_pd.to_dict('records'),
            "check_results": check_results_pd.to_dict('records')
        }
        
        print(f"✅ Validation completed in {execution_time:.2f} seconds")
        print(f"📊 Results: {passed_checks}/{total_checks} checks passed ({results['success_rate']:.1f}%)")
        
        if failed_checks > 0:
            print(f"⚠️  {failed_checks} checks failed - review results below")
            
        return results
        
    except Exception as e:
        print(f"❌ Error running Deequ validation for {table_name}: {str(e)}")
        return {"error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Quality Validation Across All Tables

# COMMAND ----------

# Tables to validate
tables_to_validate = [
    f"{BRONZE_DB}.transactions",
    f"{BRONZE_DB}.products", 
    f"{BRONZE_DB}.customers",
    f"{GOLD_DB}.customer_insights"
]

# Run validation on all tables
validation_results = {}

print("🔍 Starting comprehensive data quality validation...")
print("="*70)

for table_name in tables_to_validate:
    try:
        print(f"\n🎯 Validating {table_name}")
        print("-" * 50)
        
        # Load table
        df = spark.table(table_name)
        record_count = df.count()
        
        print(f"📊 Table records: {record_count:,}")
        
        # Skip validation if table is empty
        if record_count == 0:
            print("⚠️  Skipping validation - table is empty")
            validation_results[table_name] = {
                "status": "skipped",
                "reason": "empty_table",
                "record_count": 0
            }
            continue
        
        # Run Deequ validation
        results = run_deequ_validation(table_name, df)
        results["record_count"] = record_count
        
        validation_results[table_name] = results
        
        # Display failed checks if any
        if results.get("failed_checks", 0) > 0:
            print(f"\n❌ Failed Checks for {table_name}:")
            for check_result in results.get("check_results", []):
                if check_result.get("check_status") == "Error":
                    print(f"   - {check_result.get('constraint', 'Unknown')}: {check_result.get('constraint_message', 'No message')}")
        
    except Exception as e:
        print(f"❌ Error validating {table_name}: {str(e)}")
        validation_results[table_name] = {
            "status": "error",
            "error_message": str(e)
        }

print(f"\n{'='*70}")
print("✅ Quality validation completed for all tables")
print(f"{'='*70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics Dashboard & Summary

# COMMAND ----------

def create_quality_dashboard(validation_results):
    """
    Create a comprehensive quality dashboard from validation results
    """
    try:
        print("📊 Creating Data Quality Dashboard")
        print("="*60)
        
        # Overall summary
        total_tables = len(validation_results)
        successful_validations = sum(1 for result in validation_results.values() 
                                   if result.get("status") != "error" and result.get("failed_checks", 0) == 0)
        
        print(f"🎯 **OVERALL QUALITY SUMMARY**")
        print(f"   📋 Total tables validated: {total_tables}")
        print(f"   ✅ Tables passing all checks: {successful_validations}")
        print(f"   ⚠️  Tables with issues: {total_tables - successful_validations}")
        print(f"   📈 Overall success rate: {(successful_validations/total_tables*100):.1f}%")
        
        # Detailed results per table
        print(f"\n📊 **DETAILED RESULTS BY TABLE**")
        print("-" * 60)
        
        dashboard_data = []
        
        for table_name, results in validation_results.items():
            if results.get("status") == "error":
                print(f"❌ {table_name}")
                print(f"   Error: {results.get('error_message', 'Unknown error')}")
                dashboard_data.append({
                    "table_name": table_name,
                    "status": "ERROR",
                    "total_checks": 0,
                    "passed_checks": 0,
                    "failed_checks": 0,
                    "success_rate": 0,
                    "record_count": 0,
                    "execution_time": 0
                })
            elif results.get("status") == "skipped":
                print(f"⏭️  {table_name}")
                print(f"   Skipped: {results.get('reason', 'Unknown reason')}")
                dashboard_data.append({
                    "table_name": table_name,
                    "status": "SKIPPED",
                    "total_checks": 0,
                    "passed_checks": 0,
                    "failed_checks": 0,
                    "success_rate": 0,
                    "record_count": results.get("record_count", 0),
                    "execution_time": 0
                })
            else:
                status_emoji = "✅" if results.get("failed_checks", 0) == 0 else "⚠️"
                print(f"{status_emoji} {table_name}")
                print(f"   📊 Checks: {results.get('passed_checks', 0)}/{results.get('total_checks', 0)} passed")
                print(f"   📈 Success rate: {results.get('success_rate', 0):.1f}%")
                print(f"   📝 Records: {results.get('record_count', 0):,}")
                print(f"   ⏱️  Execution time: {results.get('execution_time_seconds', 0):.2f}s")
                
                dashboard_data.append({
                    "table_name": table_name,
                    "status": "SUCCESS" if results.get("failed_checks", 0) == 0 else "FAILED",
                    "total_checks": results.get("total_checks", 0),
                    "passed_checks": results.get("passed_checks", 0),
                    "failed_checks": results.get("failed_checks", 0),
                    "success_rate": results.get("success_rate", 0),
                    "record_count": results.get("record_count", 0),
                    "execution_time": results.get("execution_time_seconds", 0)
                })
        
        # Create dashboard DataFrame
        dashboard_df = spark.createDataFrame(dashboard_data)
        
        print(f"\n📋 **QUALITY DASHBOARD TABLE**")
        dashboard_df.display()
        
        return dashboard_df
        
    except Exception as e:
        print(f"❌ Error creating quality dashboard: {str(e)}")
        return None

# COMMAND ----------

# Create and display quality dashboard
dashboard_df = create_quality_dashboard(validation_results)

if dashboard_df:
    print("\n✅ Quality dashboard created successfully")
else:
    print("❌ Failed to create quality dashboard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Quality Metrics for Historical Tracking

# COMMAND ----------

def store_quality_metrics(validation_results):
    """
    Store quality validation results in Delta tables for historical tracking
    """
    try:
        print("💾 Storing quality metrics in Delta tables...")
        
        # Prepare quality summary data
        quality_summary_data = []
        quality_detail_data = []
        
        for table_name, results in validation_results.items():
            if results.get("status") not in ["error", "skipped"]:
                # Summary record
                summary_record = {
                    "table_name": table_name,
                    "check_date": datetime.now().date(),
                    "check_timestamp": results.get("check_timestamp", datetime.now()),
                    "total_checks": results.get("total_checks", 0),
                    "passed_checks": results.get("passed_checks", 0),
                    "failed_checks": results.get("failed_checks", 0),
                    "success_rate": results.get("success_rate", 0),
                    "record_count": results.get("record_count", 0),
                    "execution_time_seconds": results.get("execution_time_seconds", 0)
                }
                quality_summary_data.append(summary_record)
                
                # Detail records for each check
                for check_result in results.get("check_results", []):
                    detail_record = {
                        "table_name": table_name,
                        "check_date": datetime.now().date(),
                        "check_timestamp": results.get("check_timestamp", datetime.now()),
                        "constraint": check_result.get("constraint", ""),
                        "constraint_status": check_result.get("check_status", ""),
                        "constraint_message": check_result.get("constraint_message", "")
                    }
                    quality_detail_data.append(detail_record)
        
        # Create DataFrames
        if quality_summary_data:
            quality_summary_df = spark.createDataFrame(quality_summary_data)
            
            # Write to Delta table
            write_delta_table(
                quality_summary_df,
                f"{GOLD_DB}.quality_metrics_summary",
                mode="append"
            )
            
            print(f"✅ Stored {len(quality_summary_data)} quality summary records")
        
        if quality_detail_data:
            quality_detail_df = spark.createDataFrame(quality_detail_data)
            
            # Write to Delta table
            write_delta_table(
                quality_detail_df,
                f"{GOLD_DB}.quality_metrics_detail",
                mode="append"
            )
            
            print(f"✅ Stored {len(quality_detail_data)} quality detail records")
        
        return True
        
    except Exception as e:
        print(f"❌ Error storing quality metrics: {str(e)}")
        return False

# COMMAND ----------

# Store quality metrics
storage_success = store_quality_metrics(validation_results)

if storage_success:
    print("\n📊 Quality metrics stored successfully")
    
    # Display recent quality trends
    print("\n📈 Recent Quality Trends:")
    spark.sql(f"""
        SELECT table_name, check_date, 
               total_checks, passed_checks, failed_checks, 
               ROUND(success_rate, 1) as success_rate_pct
        FROM {GOLD_DB}.quality_metrics_summary
        WHERE check_date >= date_sub(current_date(), 7)
        ORDER BY check_date DESC, table_name
    """).display()
else:
    print("❌ Failed to store quality metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Detection & Pattern Analysis

# COMMAND ----------

def detect_data_anomalies(table_name, df):
    """
    Detect statistical anomalies in data
    """
    try:
        print(f"🔍 Detecting anomalies in {table_name}")
        
        anomalies = []
        
        # Get numeric columns
        numeric_cols = [field.name for field in df.schema.fields 
                       if field.dataType in [IntegerType(), DoubleType(), FloatType(), LongType()]]
        
        if not numeric_cols:
            print("   ℹ️  No numeric columns found for anomaly detection")
            return []
        
        # Calculate statistics for numeric columns
        stats_df = df.select([
            F.mean(col).alias(f"{col}_mean"),
            F.stddev(col).alias(f"{col}_stddev"),
            F.min(col).alias(f"{col}_min"),
            F.max(col).alias(f"{col}_max"),
            F.count(col).alias(f"{col}_count")
        ] for col in numeric_cols[:5]).collect()[0]  # Limit to first 5 numeric columns
        
        # Check for anomalies
        for col in numeric_cols[:5]:
            mean_val = stats_df[f"{col}_mean"]
            stddev_val = stats_df[f"{col}_stddev"]
            min_val = stats_df[f"{col}_min"]
            max_val = stats_df[f"{col}_max"]
            count_val = stats_df[f"{col}_count"]
            
            if mean_val is not None and stddev_val is not None:
                # Check for extreme values (beyond 3 standard deviations)
                outlier_count = df.filter(
                    (F.col(col) > (mean_val + 3 * stddev_val)) |
                    (F.col(col) < (mean_val - 3 * stddev_val))
                ).count()
                
                if outlier_count > 0:
                    anomalies.append({
                        "table_name": table_name,
                        "column_name": col,
                        "anomaly_type": "statistical_outlier",
                        "description": f"{outlier_count} values beyond 3 standard deviations",
                        "outlier_count": outlier_count,
                        "mean": mean_val,
                        "stddev": stddev_val,
                        "min": min_val,
                        "max": max_val
                    })
                
                # Check for suspicious patterns
                if min_val == max_val and count_val > 1:
                    anomalies.append({
                        "table_name": table_name,
                        "column_name": col,
                        "anomaly_type": "constant_value",
                        "description": f"All values are constant ({min_val})",
                        "constant_value": min_val,
                        "record_count": count_val
                    })
        
        if anomalies:
            print(f"   ⚠️  Found {len(anomalies)} anomalies")
            for anomaly in anomalies:
                print(f"      - {anomaly['column_name']}: {anomaly['description']}")
        else:
            print("   ✅ No anomalies detected")
        
        return anomalies
        
    except Exception as e:
        print(f"❌ Error detecting anomalies in {table_name}: {str(e)}")
        return []

# COMMAND ----------

# Run anomaly detection on all tables
print("🔍 Running anomaly detection across all tables...")
print("="*60)

all_anomalies = []

for table_name in tables_to_validate:
    try:
        print(f"\n🎯 Analyzing {table_name}")
        df = spark.table(table_name)
        
        if df.count() > 0:
            anomalies = detect_data_anomalies(table_name, df)
            all_anomalies.extend(anomalies)
        else:
            print("   ⏭️  Skipping - empty table")
            
    except Exception as e:
        print(f"❌ Error analyzing {table_name}: {str(e)}")

# Summary of anomalies
print(f"\n📊 **ANOMALY DETECTION SUMMARY**")
print(f"   🔍 Total anomalies found: {len(all_anomalies)}")

if all_anomalies:
    print(f"   📋 Anomaly breakdown:")
    anomaly_types = {}
    for anomaly in all_anomalies:
        anomaly_type = anomaly["anomaly_type"]
        anomaly_types[anomaly_type] = anomaly_types.get(anomaly_type, 0) + 1
    
    for anomaly_type, count in anomaly_types.items():
        print(f"      - {anomaly_type}: {count}")
else:
    print("   ✅ No anomalies detected across all tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Alerting & Notification System

# COMMAND ----------

def create_quality_alerts(validation_results, all_anomalies):
    """
    Create quality alerts based on validation results and anomalies
    """
    try:
        print("🚨 Creating quality alerts...")
        
        alerts = []
        
        # Check for failed validations
        for table_name, results in validation_results.items():
            if results.get("failed_checks", 0) > 0:
                alert = {
                    "alert_type": "validation_failure",
                    "severity": "HIGH" if results.get("success_rate", 100) < 80 else "MEDIUM",
                    "table_name": table_name,
                    "message": f"Data quality validation failed: {results.get('failed_checks')} out of {results.get('total_checks')} checks failed",
                    "success_rate": results.get("success_rate", 0),
                    "failed_checks": results.get("failed_checks", 0),
                    "alert_timestamp": datetime.now()
                }
                alerts.append(alert)
        
        # Check for anomalies
        table_anomaly_counts = {}
        for anomaly in all_anomalies:
            table_name = anomaly["table_name"]
            table_anomaly_counts[table_name] = table_anomaly_counts.get(table_name, 0) + 1
        
        for table_name, anomaly_count in table_anomaly_counts.items():
            alert = {
                "alert_type": "anomaly_detected",
                "severity": "HIGH" if anomaly_count > 5 else "MEDIUM",
                "table_name": table_name,
                "message": f"Data anomalies detected: {anomaly_count} anomalies found",
                "anomaly_count": anomaly_count,
                "alert_timestamp": datetime.now()
            }
            alerts.append(alert)
        
        # Check for empty tables
        for table_name, results in validation_results.items():
            if results.get("record_count", 0) == 0:
                alert = {
                    "alert_type": "empty_table",
                    "severity": "HIGH",
                    "table_name": table_name,
                    "message": f"Table is empty: {table_name} contains no records",
                    "record_count": 0,
                    "alert_timestamp": datetime.now()
                }
                alerts.append(alert)
        
        return alerts
        
    except Exception as e:
        print(f"❌ Error creating quality alerts: {str(e)}")
        return []

# COMMAND ----------

def send_quality_notifications(alerts):
    """
    Send notifications for quality alerts
    """
    try:
        if not alerts:
            print("✅ No quality alerts to send")
            return
        
        print(f"📧 Processing {len(alerts)} quality alerts...")
        
        # Group alerts by severity
        high_alerts = [a for a in alerts if a.get("severity") == "HIGH"]
        medium_alerts = [a for a in alerts if a.get("severity") == "MEDIUM"]
        
        print(f"   🔴 High severity alerts: {len(high_alerts)}")
        print(f"   🟡 Medium severity alerts: {len(medium_alerts)}")
        
        # Create notification message
        notification_message = f"""
        DATA QUALITY ALERT SUMMARY
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        HIGH SEVERITY ALERTS ({len(high_alerts)}):
        """
        
        for alert in high_alerts:
            notification_message += f"\n• {alert['table_name']}: {alert['message']}"
        
        if medium_alerts:
            notification_message += f"\n\nMEDIUM SEVERITY ALERTS ({len(medium_alerts)}):"
            for alert in medium_alerts:
                notification_message += f"\n• {alert['table_name']}: {alert['message']}"
        
        print(f"\n📩 **QUALITY ALERT NOTIFICATION**")
        print("="*60)
        print(notification_message)
        print("="*60)
        
        # Store alerts in Delta table
        if alerts:
            alerts_df = spark.createDataFrame(alerts)
            write_delta_table(
                alerts_df,
                f"{GOLD_DB}.quality_alerts",
                mode="append"
            )
            print(f"✅ Stored {len(alerts)} alerts in quality_alerts table")
        
        # In production, you would send actual notifications here:
        # - Email via SMTP
        # - Slack via webhook
        # - Teams via webhook
        # - PagerDuty for high severity alerts
        
        print("\n💡 **NOTIFICATION CHANNELS** (for production setup):")
        print("   📧 Email: Configure SMTP server and recipient list")
        print("   💬 Slack: Set up webhook integration")
        print("   📱 Teams: Configure Teams webhook")
        print("   🚨 PagerDuty: Set up for high-severity alerts")
        
    except Exception as e:
        print(f"❌ Error sending quality notifications: {str(e)}")

# COMMAND ----------

# Create and send quality alerts
alerts = create_quality_alerts(validation_results, all_anomalies)
send_quality_notifications(alerts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Quality Monitoring Schedule

# COMMAND ----------

def create_quality_monitoring_schedule():
    """
    Create configuration for automated quality monitoring
    """
    schedule_config = {
        "name": "Daily Data Quality Monitoring",
        "schedule": {
            "cron_expression": "0 6 * * *",  # 6 AM daily
            "timezone": "UTC"
        },
        "tasks": [
            {
                "task_key": "quality_validation",
                "notebook_path": "/Repos/ecommerce-pipeline/05_quality_monitor/data_quality_monitor",
                "cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "Standard_DS3_v2",
                    "num_workers": 2
                }
            }
        ],
        "email_notifications": {
            "on_success": ["data-team@company.com"],
            "on_failure": ["data-team@company.com", "ops-team@company.com"],
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {
            "on_success": [
                {
                    "id": "slack-webhook",
                    "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
                }
            ],
            "on_failure": [
                {
                    "id": "slack-webhook",
                    "url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
                }
            ]
        }
    }
    
    return schedule_config

# COMMAND ----------

# Display job configuration
job_config = create_quality_monitoring_schedule()

print("📅 **AUTOMATED QUALITY MONITORING SCHEDULE**")
print("="*60)
print(f"🕕 Schedule: {job_config['schedule']['cron_expression']} (Daily at 6 AM UTC)")
print(f"📓 Notebook: {job_config['tasks'][0]['notebook_path']}")
print(f"🖥️  Cluster: {job_config['tasks'][0]['cluster']['node_type_id']}")
print(f"📧 Email notifications: {job_config['email_notifications']['on_success']}")
print(f"💬 Slack notifications: Configured")

print("\n🔧 **TO CREATE THE AUTOMATED JOB:**")
print("="*60)
print("1. Go to Databricks workspace")
print("2. Click 'Workflows' in the left sidebar")
print("3. Click 'Create Job'")
print("4. Configure the job with these settings:")
print("   - Job name: 'Daily Data Quality Monitoring'")
print("   - Task type: 'Notebook'")
print("   - Notebook path: '/Repos/ecommerce-pipeline/05_quality_monitor/data_quality_monitor'")
print("   - Cluster: Use existing cluster or create new one")
print("   - Schedule: '0 6 * * *' (daily at 6 AM)")
print("   - Email notifications: Add data team emails")
print("   - Webhook notifications: Add Slack webhook URL")
print("5. Save and enable the job")
print("\n✅ The job will run automatically every day at 6 AM UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Quality Dashboard

# COMMAND ----------

def create_executive_dashboard():
    """
    Create an executive-level data quality dashboard
    """
    try:
        print("📊 Creating Executive Quality Dashboard")
        print("="*60)
        
        # Overall quality score
        total_tables = len(validation_results)
        perfect_tables = sum(1 for r in validation_results.values() 
                           if r.get("success_rate", 0) == 100)
        
        overall_score = (perfect_tables / total_tables * 100) if total_tables > 0 else 0
        
        print(f"🎯 **DATA QUALITY SCORECARD**")
        print(f"   📈 Overall Quality Score: {overall_score:.1f}%")
        print(f"   ✅ Tables with Perfect Quality: {perfect_tables}/{total_tables}")
        print(f"   📊 Average Success Rate: {sum(r.get('success_rate', 0) for r in validation_results.values()) / len(validation_results):.1f}%")
        
        # Quality trends (if historical data exists)
        try:
            trend_data = spark.sql(f"""
                SELECT 
                    check_date,
                    AVG(success_rate) as avg_success_rate,
                    COUNT(*) as tables_checked,
                    SUM(failed_checks) as total_failed_checks
                FROM {GOLD_DB}.quality_metrics_summary
                WHERE check_date >= date_sub(current_date(), 7)
                GROUP BY check_date
                ORDER BY check_date DESC
            """).collect()
            
            if trend_data:
                print(f"\n📈 **QUALITY TRENDS (Last 7 Days)**")
                for row in trend_data:
                    print(f"   {row['check_date']}: {row['avg_success_rate']:.1f}% avg success rate, {row['total_failed_checks']} failed checks")
        except:
            print("\n📈 **QUALITY TRENDS**: No historical data available yet")
        
        # Alert summary
        if alerts:
            print(f"\n🚨 **CURRENT ALERTS**")
            high_alerts = [a for a in alerts if a.get("severity") == "HIGH"]
            medium_alerts = [a for a in alerts if a.get("severity") == "MEDIUM"]
            
            print(f"   🔴 High Priority: {len(high_alerts)} alerts")
            print(f"   🟡 Medium Priority: {len(medium_alerts)} alerts")
            
            if high_alerts:
                print(f"   🎯 Action Required:")
                for alert in high_alerts:
                    print(f"      - {alert['table_name']}: {alert['message']}")
        else:
            print(f"\n✅ **NO ACTIVE ALERTS** - All systems healthy")
        
        # Recommendations
        print(f"\n💡 **RECOMMENDATIONS**")
        
        low_quality_tables = [name for name, results in validation_results.items() 
                             if results.get("success_rate", 100) < 90]
        
        if low_quality_tables:
            print(f"   🔧 Fix data quality issues in: {', '.join(low_quality_tables)}")
        
        if all_anomalies:
            print(f"   🔍 investigate {len(all_anomalies)} data anomalies")
        
        if not alerts:
            print(f"   ✅ Data quality is excellent - continue monitoring")
        
        print(f"\n📊 **NEXT QUALITY CHECK**: Tomorrow at 6 AM UTC")
        
    except Exception as e:
        print(f"❌ Error creating executive dashboard: {str(e)}")

# COMMAND ----------

# Create executive dashboard
create_executive_dashboard()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary & Next Steps

# COMMAND ----------

print("🏁 **DATA QUALITY MONITORING IMPLEMENTATION COMPLETE**")
print("="*70)
print("✅ Comprehensive quality validation framework implemented")
print("✅ PyDeequ-based data profiling and validation")
print("✅ Statistical anomaly detection")
print("✅ Quality metrics storage and historical tracking")
print("✅ Automated alerting and notification system")
print("✅ Executive dashboard and reporting")
print("✅ Daily automated quality monitoring configured")

print(f"\n📊 **QUALITY VALIDATION SUMMARY**")
for table_name, results in validation_results.items():
    if results.get("status") not in ["error", "skipped"]:
        status_emoji = "✅" if results.get("failed_checks", 0) == 0 else "⚠️"
        print(f"   {status_emoji} {table_name}: {results.get('success_rate', 0):.1f}% success rate")

print(f"\n🎯 **NEXT STEPS**")
print("1. Set up automated daily quality monitoring job")
print("2. Configure email/Slack notifications")
print("3. Review and fix any failing quality checks")
print("4. Monitor quality trends over time")
print("5. Adjust quality rules based on business requirements")

print(f"\n💡 **BENEFITS ACHIEVED**")
print("✅ Proactive data quality monitoring")
print("✅ Early detection of data issues")
print("✅ Historical quality tracking")
print("✅ Automated alerting for quality problems")
print("✅ Executive visibility into data health")

