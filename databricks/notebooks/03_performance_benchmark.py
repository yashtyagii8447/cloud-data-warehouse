
from pyspark.sql import *
from pyspark.sql.functions import *
import time
import pandas as pd


spark = SparkSession.builder.appName("FreeTierBenchmark").getOrCreate()


spark_df = spark.read.parquet("/tmp/ecommerce_data.parquet")


sample_size = 2000

pandas_df = spark_df.limit(sample_size).toPandas()

# PySpark processing
start_time = time.time()
spark_result = spark_df \
    .filter(col("revenue") > 0) \
    .withColumn("profit", col("revenue") - col("cost")) \
    .withColumn("profit_margin", col("profit") / col("revenue")) \
    .count()
spark_time = time.time() - start_time


# Pandas processing
start_time = time.time()
pandas_result = pandas_df[pandas_df['revenue'] > 0].copy()
pandas_result['profit'] = pandas_result['revenue'] - pandas_result['cost']
pandas_result['profit_margin'] = pandas_result['profit'] / pandas_result['revenue']
pandas_count = len(pandas_result)
pandas_time = time.time() - start_time



# Calculate improvement
improvement = ((pandas_time - spark_time) / pandas_time) * 100

print("=== PERFORMANCE BENCHMARK ===")
print(f"Dataset size: {sample_size} records")
print(f"PySpark time: {spark_time:.2f} seconds")
print(f"Pandas time: {pandas_time:.2f} seconds")
print(f"Performance improvement: {improvement:.2f}%")

# Memory usage comparison
spark_memory = spark.sparkContext.getConf().get("spark.executor.memory")
print(f"Spark executor memory: {spark_memory}")
print(f"Pandas memory usage: {pandas_df.memory_usage(deep=True).sum() / (1024 ** 2):.2f} MB")