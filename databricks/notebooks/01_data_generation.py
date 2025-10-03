from pyspark.sql import *
from pyspark.sql.functions import *
import math

spark = SparkSession.builder.appName("DataGenerationFree_New").getOrCreate()

max_records = 10_00_000  

df = spark.range(0, max_records).toDF("transaction_id")
df = df.withColumn("product_id", (rand() * 50).cast("int"))
df = df.withColumn("customer_id", (rand() * 2000).cast("int"))
df = df.withColumn("revenue", (rand() * 100 + 5).cast("float"))
df = df.withColumn("cost", (rand() * 60 + 3).cast("float"))
df = df.withColumn("timestamp", expr("current_timestamp()"))
df = df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))


approx_size_mb = (max_records * 1000) / (1024 * 1024)
print(f"Generated {max_records} records, approx {approx_size_mb:.2f} MB uncompressed")


df.write.mode("overwrite").parquet("/tmp/ecommerce_data.parquet")

print("Data generation completed. Ready for ETL processing.")
