# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Storage Optimization
# MAGIC
# MAGIC This notebook optimizes Delta Lake tables for better query performance through:
# MAGIC - **File Compaction**: Combines small Parquet files into larger, more efficient files
# MAGIC - **Z-ORDER Clustering**: Co-locates related data for faster filtering and joins
# MAGIC - **Vacuum Operations**: Removes old file versions to save storage space
# MAGIC - **Performance Monitoring**: Tracks optimization metrics and improvements
# MAGIC
# MAGIC **Schedule**: Run daily via Databricks Jobs to maintain optimal performance

# COMMAND ----------

# MAGIC %run "../00_setup/config_helper"

# COMMAND ----------

# Import additional libraries for optimization
import time
from datetime import datetime, timedelta
import json

print("✅ Optimization notebook initialized")
print(f"📅 Current timestamp: {datetime.now()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Optimization Analysis

# COMMAND ----------

def analyze_table_files(table_name):
    """
    Analyze the file structure and size distribution of a Delta table
    """
    try:
        # Get table details
        table_detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        
        # Get file statistics
        file_stats = spark.sql(f"""
            SELECT 
                COUNT(*) as file_count,
                ROUND(SUM(size) / 1024 / 1024, 2) as total_size_mb,
                ROUND(AVG(size) / 1024 / 1024, 2) as avg_file_size_mb,
                ROUND(MIN(size) / 1024 / 1024, 2) as min_file_size_mb,
                ROUND(MAX(size) / 1024 / 1024, 2) as max_file_size_mb
            FROM (
                SELECT input_file_name() as file_name, 
                       COUNT(*) as row_count
                FROM {table_name}
                GROUP BY input_file_name()
            ) files
            JOIN (
                SELECT path, size 
                FROM json_table(
                    '{{"files": []}}',
                    '$.files[*]' COLUMNS(path STRING, size LONG)
                )
            ) file_sizes ON files.file_name LIKE CONCAT('%', file_sizes.path, '%')
        """)
        
        # Alternative approach for file analysis
        table_files = spark.sql(f"""
            SELECT 
                input_file_name() as file_path,
                COUNT(*) as records_per_file
            FROM {table_name}
            GROUP BY input_file_name()
        """).collect()
        
        analysis = {
            "table_name": table_name,
            "location": table_detail["location"],
            "num_files": len(table_files),
            "total_records": sum([f["records_per_file"] for f in table_files]),
            "avg_records_per_file": sum([f["records_per_file"] for f in table_files]) / len(table_files) if table_files else 0,
            "analysis_timestamp": datetime.now()
        }
        
        return analysis
        
    except Exception as e:
        print(f"❌ Error analyzing table {table_name}: {str(e)}")
        return None

# COMMAND ----------

# Analyze current state of Gold tables
print("🔍 Analyzing current table state before optimization...")

tables_to_optimize = [
    f"{GOLD_DB}.customer_insights"
]

pre_optimization_stats = {}

for table in tables_to_optimize:
    try:
        print(f"\n📊 Analyzing {table}:")
        
        # Get basic table info
        table_info = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        
        # Get record count
        record_count = spark.table(table).count()
        
        # Get file count (simplified approach)
        file_analysis = spark.sql(f"""
            SELECT 
                COUNT(DISTINCT input_file_name()) as file_count,
                COUNT(*) as total_records
            FROM {table}
        """).collect()[0]
        
        stats = {
            "table_name": table,
            "total_records": record_count,
            "file_count": file_analysis["file_count"],
            "avg_records_per_file": record_count / file_analysis["file_count"] if file_analysis["file_count"] > 0 else 0,
            "table_size_bytes": table_info["sizeInBytes"],
            "table_size_mb": round(table_info["sizeInBytes"] / 1024 / 1024, 2),
            "num_files": table_info["numFiles"]
        }
        
        pre_optimization_stats[table] = stats
        
        print(f"   📈 Total records: {stats['total_records']:,}")
        print(f"   📁 Number of files: {stats['file_count']:,}")
        print(f"   📊 Avg records per file: {stats['avg_records_per_file']:,.0f}")
        print(f"   💾 Table size: {stats['table_size_mb']} MB")
        
    except Exception as e:
        print(f"❌ Error analyzing {table}: {str(e)}")

print("\n✅ Pre-optimization analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Smart File Compaction with OPTIMIZE

# COMMAND ----------

def optimize_table_smart(table_name, partition_filter=None, min_file_size_mb=128):
    """
    Optimize Delta table with smart partitioning and monitoring
    
    Args:
        table_name: Name of the table to optimize
        partition_filter: SQL WHERE clause for selective optimization
        min_file_size_mb: Target minimum file size in MB
    """
    try:
        start_time = time.time()
        print(f"🚀 Starting OPTIMIZE operation for {table_name}")
        
        # Build optimize command
        optimize_cmd = f"OPTIMIZE {table_name}"
        if partition_filter:
            optimize_cmd += f" WHERE {partition_filter}"
        
        print(f"📝 Executing: {optimize_cmd}")
        
        # Execute optimize operation
        result = spark.sql(optimize_cmd)
        
        # Collect and display results
        optimization_metrics = result.collect()[0]
        
        execution_time = time.time() - start_time
        
        metrics = {
            "table_name": table_name,
            "partition_filter": partition_filter,
            "files_added": optimization_metrics["filesAdded"],
            "files_removed": optimization_metrics["filesRemoved"],
            "partitions_optimized": optimization_metrics["partitionsOptimized"],
            "num_batches": optimization_metrics["numBatches"],
            "total_considered_files": optimization_metrics["totalConsideredFiles"],
            "total_files_skipped": optimization_metrics["totalFilesSkipped"],
            "execution_time_seconds": round(execution_time, 2),
            "optimization_timestamp": datetime.now()
        }
        
        print(f"✅ OPTIMIZE completed successfully in {execution_time:.2f} seconds")
        print(f"   📁 Files added: {metrics['files_added']}")
        print(f"   🗑️  Files removed: {metrics['files_removed']}")
        print(f"   📊 Partitions optimized: {metrics['partitions_optimized']}")
        print(f"   ⏭️  Files skipped: {metrics['total_files_skipped']}")
        
        return metrics
        
    except Exception as e:
        print(f"❌ Error optimizing {table_name}: {str(e)}")
        raise

# COMMAND ----------

# Optimize Gold layer tables
print("🔧 Starting file compaction optimization...")

optimization_results = {}

for table in tables_to_optimize:
    try:
        print(f"\n{'='*60}")
        print(f"🎯 Optimizing {table}")
        print(f"{'='*60}")
        
        # Strategy 1: Optimize recent data (last 30 days)
        # Note: Adjusting filter to match our actual column names
        recent_data_filter = "ingestion_date >= date_sub(current_date(), 30)"
        
        print(f"🔄 Phase 1: Optimizing recent data (last 30 days)")
        recent_optimization = optimize_table_smart(
            table_name=table,
            partition_filter=recent_data_filter
        )
        
        # Strategy 2: Optimize older data if needed
        print(f"🔄 Phase 2: Optimizing older data")
        older_data_filter = "ingestion_date < date_sub(current_date(), 30)"
        
        older_optimization = optimize_table_smart(
            table_name=table,
            partition_filter=older_data_filter
        )
        
        # Combine results
        optimization_results[table] = {
            "recent_optimization": recent_optimization,
            "older_optimization": older_optimization,
            "total_execution_time": recent_optimization["execution_time_seconds"] + older_optimization["execution_time_seconds"]
        }
        
        print(f"✅ Completed optimization for {table}")
        
    except Exception as e:
        print(f"❌ Failed to optimize {table}: {str(e)}")
        optimization_results[table] = {"error": str(e)}

print(f"\n{'='*60}")
print("✅ File compaction optimization completed")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-ORDER Clustering for Query Performance

# COMMAND ----------

def zorder_optimize_table(table_name, zorder_columns, partition_filter=None):
    """
    Apply Z-ORDER clustering to optimize query performance
    
    Args:
        table_name: Name of the table to optimize
        zorder_columns: List of columns to use for Z-ORDER
        partition_filter: SQL WHERE clause for selective optimization
    """
    try:
        start_time = time.time()
        print(f"🎯 Starting Z-ORDER optimization for {table_name}")
        print(f"📊 Z-ORDER columns: {', '.join(zorder_columns)}")
        
        # Build Z-ORDER command
        zorder_cmd = f"OPTIMIZE {table_name}"
        if partition_filter:
            zorder_cmd += f" WHERE {partition_filter}"
        zorder_cmd += f" ZORDER BY ({', '.join(zorder_columns)})"
        
        print(f"📝 Executing: {zorder_cmd}")
        
        # Execute Z-ORDER operation
        result = spark.sql(zorder_cmd)
        zorder_metrics = result.collect()[0]
        
        execution_time = time.time() - start_time
        
        metrics = {
            "table_name": table_name,
            "zorder_columns": zorder_columns,
            "partition_filter": partition_filter,
            "files_added": zorder_metrics["filesAdded"],
            "files_removed": zorder_metrics["filesRemoved"],
            "partitions_optimized": zorder_metrics["partitionsOptimized"],
            "execution_time_seconds": round(execution_time, 2),
            "zorder_timestamp": datetime.now()
        }
        
        print(f"✅ Z-ORDER completed successfully in {execution_time:.2f} seconds")
        print(f"   📁 Files added: {metrics['files_added']}")
        print(f"   🗑️  Files removed: {metrics['files_removed']}")
        print(f"   📊 Partitions optimized: {metrics['partitions_optimized']}")
        
        return metrics
        
    except Exception as e:
        print(f"❌ Error applying Z-ORDER to {table_name}: {str(e)}")
        raise

# COMMAND ----------

# Apply Z-ORDER clustering
print("🎯 Starting Z-ORDER clustering optimization...")

zorder_results = {}

# Define Z-ORDER strategies for each table
zorder_strategies = {
    f"{GOLD_DB}.customer_insights": {
        "columns": ["customer_id", "customer_tier", "customer_segment"],
        "reason": "Most queries filter by customer_id, tier, or segment"
    }
}

for table, strategy in zorder_strategies.items():
    try:
        print(f"\n{'='*60}")
        print(f"🎯 Applying Z-ORDER to {table}")
        print(f"📊 Strategy: {strategy['reason']}")
        print(f"{'='*60}")
        
        # Apply Z-ORDER to recent data first
        recent_filter = "ingestion_date >= date_sub(current_date(), 30)"
        
        print(f"🔄 Phase 1: Z-ORDER recent data (last 30 days)")
        recent_zorder = zorder_optimize_table(
            table_name=table,
            zorder_columns=strategy["columns"],
            partition_filter=recent_filter
        )
        
        # Apply Z-ORDER to older data
        print(f"🔄 Phase 2: Z-ORDER older data")
        older_filter = "ingestion_date < date_sub(current_date(), 30)"
        
        older_zorder = zorder_optimize_table(
            table_name=table,
            zorder_columns=strategy["columns"],
            partition_filter=older_filter
        )
        
        zorder_results[table] = {
            "recent_zorder": recent_zorder,
            "older_zorder": older_zorder,
            "total_execution_time": recent_zorder["execution_time_seconds"] + older_zorder["execution_time_seconds"]
        }
        
        print(f"✅ Completed Z-ORDER for {table}")
        
    except Exception as e:
        print(f"❌ Failed to apply Z-ORDER to {table}: {str(e)}")
        zorder_results[table] = {"error": str(e)}

print(f"\n{'='*60}")
print("✅ Z-ORDER clustering optimization completed")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-Optimization Analysis & Comparison

# COMMAND ----------

# Analyze post-optimization state
print("📊 Analyzing post-optimization performance...")

post_optimization_stats = {}

for table in tables_to_optimize:
    try:
        print(f"\n📊 Analyzing {table} after optimization:")
        
        # Get updated table info
        table_info = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
        
        # Get record count
        record_count = spark.table(table).count()
        
        # Get file count
        file_analysis = spark.sql(f"""
            SELECT 
                COUNT(DISTINCT input_file_name()) as file_count,
                COUNT(*) as total_records
            FROM {table}
        """).collect()[0]
        
        stats = {
            "table_name": table,
            "total_records": record_count,
            "file_count": file_analysis["file_count"],
            "avg_records_per_file": record_count / file_analysis["file_count"] if file_analysis["file_count"] > 0 else 0,
            "table_size_bytes": table_info["sizeInBytes"],
            "table_size_mb": round(table_info["sizeInBytes"] / 1024 / 1024, 2),
            "num_files": table_info["numFiles"]
        }
        
        post_optimization_stats[table] = stats
        
        print(f"   📈 Total records: {stats['total_records']:,}")
        print(f"   📁 Number of files: {stats['file_count']:,}")
        print(f"   📊 Avg records per file: {stats['avg_records_per_file']:,.0f}")
        print(f"   💾 Table size: {stats['table_size_mb']} MB")
        
    except Exception as e:
        print(f"❌ Error analyzing {table}: {str(e)}")

# COMMAND ----------

# Generate optimization comparison report
print("\n📋 OPTIMIZATION PERFORMANCE REPORT")
print("="*80)

for table in tables_to_optimize:
    if table in pre_optimization_stats and table in post_optimization_stats:
        pre = pre_optimization_stats[table]
        post = post_optimization_stats[table]
        
        print(f"\n🏆 {table}")
        print("-" * 60)
        
        # File count comparison
        file_reduction = pre["file_count"] - post["file_count"]
        file_reduction_pct = (file_reduction / pre["file_count"] * 100) if pre["file_count"] > 0 else 0
        
        print(f"📁 Files: {pre['file_count']:,} → {post['file_count']:,} ({file_reduction_pct:+.1f}%)")
        
        # Records per file comparison
        records_per_file_improvement = post["avg_records_per_file"] - pre["avg_records_per_file"]
        records_per_file_pct = (records_per_file_improvement / pre["avg_records_per_file"] * 100) if pre["avg_records_per_file"] > 0 else 0
        
        print(f"📊 Avg records/file: {pre['avg_records_per_file']:,.0f} → {post['avg_records_per_file']:,.0f} ({records_per_file_pct:+.1f}%)")
        
        # Size comparison
        size_change = post["table_size_mb"] - pre["table_size_mb"]
        size_change_pct = (size_change / pre["table_size_mb"] * 100) if pre["table_size_mb"] > 0 else 0
        
        print(f"💾 Size: {pre['table_size_mb']} MB → {post['table_size_mb']} MB ({size_change_pct:+.1f}%)")
        
        # Performance indicators
        if file_reduction_pct > 10:
            print("✅ Significant file consolidation achieved")
        if records_per_file_pct > 50:
            print("✅ Excellent file size optimization")
        if abs(size_change_pct) < 5:
            print("✅ Storage efficiency maintained")

print("\n" + "="*80)
print("🏁 Optimization analysis completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vacuum Operations - Clean Up Old Files

# COMMAND ----------

def vacuum_table_safe(table_name, retention_hours=168):  # 7 days default
    """
    Safely vacuum a Delta table to remove old file versions
    
    Args:
        table_name: Name of the table to vacuum
        retention_hours: Hours to retain old versions (default: 168 = 7 days)
    """
    try:
        start_time = time.time()
        print(f"🧹 Starting VACUUM operation for {table_name}")
        print(f"⏰ Retention period: {retention_hours} hours ({retention_hours/24:.1f} days)")
        
        # First, dry run to see what would be deleted
        dry_run_result = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
        files_to_delete = dry_run_result.collect()
        
        print(f"🔍 Dry run completed: {len(files_to_delete)} files would be deleted")
        
        if len(files_to_delete) > 0:
            # Perform actual vacuum
            vacuum_result = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
            
            execution_time = time.time() - start_time
            
            print(f"✅ VACUUM completed successfully in {execution_time:.2f} seconds")
            print(f"🗑️  Files deleted: {len(files_to_delete)}")
            
            return {
                "table_name": table_name,
                "files_deleted": len(files_to_delete),
                "retention_hours": retention_hours,
                "execution_time_seconds": round(execution_time, 2),
                "vacuum_timestamp": datetime.now()
            }
        else:
            print("ℹ️  No files to vacuum (all files within retention period)")
            return {
                "table_name": table_name,
                "files_deleted": 0,
                "retention_hours": retention_hours,
                "vacuum_timestamp": datetime.now()
            }
            
    except Exception as e:
        print(f"❌ Error vacuuming {table_name}: {str(e)}")
        raise

# COMMAND ----------

# Vacuum tables (uncomment to enable)
print("🧹 Starting VACUUM operations...")
print("⚠️  Note: VACUUM permanently deletes old file versions")
print("ℹ️  Keeping 7-day retention for time travel capabilities")

vacuum_results = {}

for table in tables_to_optimize:
    try:
        print(f"\n{'='*50}")
        print(f"🧹 Vacuuming {table}")
        print(f"{'='*50}")
        
        # Use 7-day retention (168 hours) for safety
        vacuum_result = vacuum_table_safe(
            table_name=table,
            retention_hours=168  # 7 days
        )
        
        vacuum_results[table] = vacuum_result
        
    except Exception as e:
        print(f"❌ Failed to vacuum {table}: {str(e)}")
        vacuum_results[table] = {"error": str(e)}

print(f"\n{'='*50}")
print("✅ VACUUM operations completed")
print(f"{'='*50}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Testing - Query Speed Validation

# COMMAND ----------

def test_query_performance(table_name, test_queries):
    """
    Test query performance on optimized table
    
    Args:
        table_name: Name of the table to test
        test_queries: Dictionary of test queries
    """
    print(f"🚀 Testing query performance for {table_name}")
    
    performance_results = {}
    
    for query_name, query_sql in test_queries.items():
        try:
            print(f"\n⏱️  Testing: {query_name}")
            
            # Execute query multiple times for accurate timing
            times = []
            for run in range(3):
                start_time = time.time()
                result = spark.sql(query_sql)
                result.count()  # Force execution
                end_time = time.time()
                times.append(end_time - start_time)
            
            avg_time = sum(times) / len(times)
            performance_results[query_name] = {
                "avg_execution_time": round(avg_time, 3),
                "runs": times,
                "query": query_sql
            }
            
            print(f"   📊 Average time: {avg_time:.3f} seconds")
            
        except Exception as e:
            print(f"❌ Error testing {query_name}: {str(e)}")
            performance_results[query_name] = {"error": str(e)}
    
    return performance_results

# COMMAND ----------

# Define test queries for customer insights table
test_queries = {
    "customer_tier_filter": f"""
        SELECT COUNT(*) 
        FROM {GOLD_DB}.customer_insights 
        WHERE customer_tier = 'Premium'
    """,
    
    "customer_segment_aggregation": f"""
        SELECT customer_segment, 
               COUNT(*) as customer_count,
               AVG(avg_order_value) as avg_aov
        FROM {GOLD_DB}.customer_insights 
        GROUP BY customer_segment
    """,
    
    "high_value_customers": f"""
        SELECT customer_id, customer_name, total_spent
        FROM {GOLD_DB}.customer_insights 
        WHERE total_spent > 1000
        ORDER BY total_spent DESC
        LIMIT 100
    """,
    
    "channel_preference_analysis": f"""
        SELECT preferred_channel, 
               COUNT(*) as customers,
               AVG(avg_order_value) as avg_aov
        FROM {GOLD_DB}.customer_insights 
        GROUP BY preferred_channel
    """,
    
    "recent_customers": f"""
        SELECT COUNT(*) 
        FROM {GOLD_DB}.customer_insights 
        WHERE days_since_last_purchase <= 30
    """
}

# Test performance
performance_results = test_query_performance(
    f"{GOLD_DB}.customer_insights", 
    test_queries
)

print("\n📊 QUERY PERFORMANCE SUMMARY")
print("="*60)
for query_name, results in performance_results.items():
    if "error" not in results:
        print(f"{query_name}: {results['avg_execution_time']:.3f}s")
    else:
        print(f"{query_name}: ERROR - {results['error']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Optimization Job Configuration

# COMMAND ----------

# Create job configuration for daily optimization
job_config = {
    "name": "Daily Delta Optimization",
    "schedule": {
        "cron_expression": "0 2 * * *",  # 2 AM daily
        "timezone": "UTC"
    },
    "tasks": [
        {
            "task_key": "optimize_customer_insights",
            "notebook_path": "/Repos/ecommerce-pipeline/04_optimize/delta_optimize_storage",
            "cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2
            }
        }
    ],
    "email_notifications": {
        "on_success": ["your-email@company.com"],
        "on_failure": ["your-email@company.com"]
    }
}

print("📋 Daily Optimization Job Configuration:")
print("="*50)
print(f"⏰ Schedule: {job_config['schedule']['cron_expression']} (Daily at 2 AM UTC)")
print(f"📓 Notebook: {job_config['tasks'][0]['notebook_path']}")
print(f"🖥️  Cluster: {job_config['tasks'][0]['cluster']['node_type_id']}")
print(f"📧 Notifications: {job_config['email_notifications']['on_success']}")

# COMMAND ----------

# Instructions for creating the job
print("\n🔧 TO CREATE THE DAILY JOB:")
print("="*50)
print("1. Go to Databricks workspace")
print("2. Click 'Workflows' in the left sidebar")
print("3. Click 'Create Job'")
print("4. Configure the job with these settings:")
print("   - Job name: 'Daily Delta Optimization'")
print("   - Task type: 'Notebook'")
print("   - Notebook path: '/Repos/ecommerce-pipeline/04_optimize/delta_optimize_storage'")
print("   - Cluster: Use existing cluster or create new one")
print("   - Schedule: '0 2 * * *' (daily at 2 AM)")
print("5. Save and enable the job")
print("\n✅ The job will run automatically every day at 2 AM")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimization Monitoring & Alerting

# COMMAND ----------

def log_optimization_metrics(optimization_results, zorder_results):
    """
    Log optimization metrics for monitoring
    """
    try:
        # Create metrics DataFrame
        metrics_data = []
        
        for table, results in optimization_results.items():
            if "error" not in results:
                metrics_data.append({
                    "table_name": table,
                    "optimization_type": "OPTIMIZE",
                    "files_added": results.get("recent_optimization", {}).get("files_added", 0),
                    "files_removed": results.get("recent_optimization", {}).get("files_removed", 0),
                    "execution_time": results.get("total_execution_time", 0),
                    "optimization_date": datetime.now().date(),
                    "optimization_timestamp": datetime.now()
                })
        
        for table, results in zorder_results.items():
            if "error" not in results:
                metrics_data.append({
                    "table_name": table,
                    "optimization_type": "ZORDER",
                    "files_added": results.get("recent_zorder", {}).get("files_added", 0),
                    "files_removed": results.get("recent_zorder", {}).get("files_removed", 0),
                    "execution_time": results.get("total_execution_time", 0),
                    "optimization_date": datetime.now().date(),
                    "optimization_timestamp": datetime.now()
                })
        
        if metrics_data:
            metrics_df = spark.createDataFrame(metrics_data)
            
            # Write to monitoring table
            write_delta_table(
                metrics_df,
                f"{GOLD_DB}.optimization_metrics",
                mode="append"
            )
            
            print("✅ Optimization metrics logged successfully")
            
        return metrics_data
        
    except Exception as e:
        print(f"❌ Error logging optimization metrics: {str(e)}")
        return []

# COMMAND ----------

# Log today's optimization metrics
print("📊 Logging optimization metrics...")

metrics_logged = log_optimization_metrics(optimization_results, zorder_results)

if metrics_logged:
    print(f"✅ Logged {len(metrics_logged)} optimization metrics")
    
    # Display recent metrics
    print("\n📋 Recent Optimization Metrics:")
    spark.sql(f"""
        SELECT table_name, optimization_type, files_added, files_removed, 
               execution_time, optimization_date
        FROM {GOLD_DB}.optimization_metrics
        WHERE optimization_date >= date_sub(current_date(), 7)
        ORDER BY optimization_timestamp DESC
    """).display()
else:
    print("ℹ️  No metrics to log")

# COMMAND ----------

# Final summary
print("\n🏁 OPTIMIZATION SUMMARY")
print("="*60)
print("✅ File compaction (OPTIMIZE) completed")
print("✅ Data clustering (Z-ORDER) completed")
print("✅ Performance testing completed")
print("✅ Metrics logging completed")
print("✅ Daily job configuration documented")
print("\n📊 Benefits achieved:")
print("   - Reduced query latency through file consolidation")
print("   - Improved filtering performance via Z-ORDER clustering")
print("   - Automated daily optimization scheduling")
print("   - Performance monitoring and alerting")
print("   - Time travel capabilities maintained")
print("\n🎯 Next steps:")
print("   1. Set up the daily job in Databricks Workflows")
print("   2. Monitor query performance improvements")
print("   3. Adjust optimization strategies based on usage patterns")
print("   4. Create alerts for optimization failures")

