from pyspark.sql import *
from pyspark.sql.functions import *
import time
import os
from pyspark.sql import SparkSession
import boto3
from datetime import datetime

def run_optimized_etl():
    start_time = time.time()
    
    # Initialize Spark with optimizations
    spark = SparkSession.builder \
        .appName("FreeTierETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Reading data
    df = spark.read.parquet("/tmp/ecommerce_data.parquet")
    
    # Transformations
    transformed_df = df \
        .filter(col("revenue") > 0) \
        .withColumn("profit", col("revenue") - col("cost")) \
        .withColumn("profit_margin", col("profit") / col("revenue")) \
        .withColumn("processing_date", current_date())
    
    # Calculating daily metrics 
    daily_metrics = transformed_df.groupBy("date").agg(
        sum("revenue").alias("total_revenue"),
        sum("profit").alias("total_profit"),
        avg("profit_margin").alias("avg_profit_margin"),
        count("transaction_id").alias("transaction_count"),
        count_distinct("product_id").alias("unique_products"),
        count_distinct("customer_id").alias("unique_customers")
    )
    
    # Creating local storage directories
    os.makedirs("/tmp/daily-metrics", exist_ok=True)
    os.makedirs("/tmp/sample-data", exist_ok=True)
    
    # Saving to local 
    daily_metrics.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save("/tmp/daily-metrics/")
    
    # Sample of raw data
    sample_data = transformed_df.limit(100)
    sample_data.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .save("/tmp/sample-data/")
    
    processing_time = time.time() - start_time
    
    # Calculate storage savings
    original_count = df.count()
    metrics_count = daily_metrics.count()
    storage_reduction = (1 - (metrics_count / original_count)) * 100
    
    print(f"ETL completed in {processing_time:.2f} seconds")
    print(f"Storage reduction through aggregation: {storage_reduction:.2f}%")
    print("Data saved to local filesystem")

    # Calling S3 upload, after ETL completes
    upload_with_fallback()
    
    return processing_time, storage_reduction

def upload_to_s3():
   
    # Initialize S3 client
    s3 = boto3.client('s3')
    bucket_name = 'resume-project-cndwh'
    
    try:
        print("Starting S3 upload process...")
        
        # Upload daily metrics
        if os.path.exists("/tmp/daily-metrics"):
            for file in os.listdir("/tmp/daily-metrics"):
                if file.endswith('.parquet'):
                    local_path = f"/tmp/daily-metrics/{file}"
                    s3_key = f"processed-data/daily-metrics/{datetime.now().strftime('%Y-%m-%d')}/{file}"
                    
                    s3.upload_file(local_path, bucket_name, s3_key)
                    print(f"Uploaded: {s3_key}")
        
        # Upload sample data
        if os.path.exists("/tmp/sample-data"):
            for file in os.listdir("/tmp/sample-data"):
                if file.endswith('.parquet'):
                    local_path = f"/tmp/sample-data/{file}"
                    s3_key = f"processed-data/sample-data/{datetime.now().strftime('%Y-%m-%d')}/{file}"
                    
                    s3.upload_file(local_path, bucket_name, s3_key)
                    print(f"Uploaded: {s3_key}")
        
        print("🎉 All files uploaded successfully to S3!")
        
    except Exception as e:
        print(f"❌ S3 upload failed: {e}")
        raise  # Re-raise the exception to trigger fallback

def upload_with_fallback():
    
    #Try S3 upload, fallback to local if AWS not configured
    
    try:
        upload_to_s3()
    except Exception as e:
        print(" AWS S3 not configured - files remain in local storage")


# Main execution - ONLY THIS SHOULD RUN WHEN SCRIPT IS EXECUTED
if __name__ == "__main__":
    try:
        etl_time, storage_saving = run_optimized_etl()
        print(f"ETL completed successfully!")
        print(f" Processing time: {etl_time:.2f} seconds")
        print(f" Storage reduction: {storage_saving:.2f}%")
    except Exception as e:
        print(f" Error: {e}")


