# task1_big_data_analysis.py

# CODTECH INTERNSHIP - TASK 1
# Perform analysis on a large dataset using PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CodTech Big Data Analysis Task") \
    .getOrCreate()

# Load dataset (ensure sample_data.csv is in the same directory)
df = spark.read.csv("C:/Users/BLESSY MIRACULINE/Downloads/sample_data.csv", header=True, inferSchema=True)


# Show schema
print("Data Schema:")
df.printSchema()

# Show sample data
print("Preview of data:")
df.show(5)

# Drop missing values
df_clean = df.dropna()

# Summary statistics
print("Summary statistics:")
df_clean.describe().show()

# Group by category and count
if 'category' in df_clean.columns:
    print("Record count by category:")
    df_clean.groupBy("category").agg(count("*").alias("record_count")).show()

# Average calculation for numeric fields
numeric_columns = [field.name for field in df_clean.schema.fields if str(field.dataType) in ['IntegerType', 'DoubleType']]
for colname in numeric_columns:
    df_clean.select(avg(col(colname)).alias(f"avg_{colname}")).show()

# Stop Spark session
spark.stop()

print("Task 1 completed.")
