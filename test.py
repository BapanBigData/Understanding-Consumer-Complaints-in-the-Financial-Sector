import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Print PySpark version to check if it's correctly installed
print("PySpark Version:", pyspark.__version__)

# # Create a Spark session to test Spark functionality
# spark = SparkSession.builder.appName("Test").getOrCreate()

spark = SparkSession.builder \
        .appName("Test") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.master", "local[*]") \
        .getOrCreate()


# Check if the Spark session was created successfully
print("Spark Session created successfully!")

# Step 1: Create a DataFrame with sample data
data = [Row(name="Alice", age=29),
        Row(name="Bob", age=31),
        Row(name="Catherine", age=35)]

df = spark.createDataFrame(data)

# Step 2: Create a temporary table/view
df.createOrReplaceTempView("people")

# Step 3: Query the table using Spark SQL
result_df = spark.sql("SELECT * FROM people")

# Show the result of the query
result_df.show()

print("===================**********=======================")

# data = [("Alice", 29), ("Bob", 31), ("Catherine", 35)]
# df = spark.createDataFrame(data, ["name", "age"])
# df.show()

# Path to your CSV file
csv_file_path = 'complaints-2024-09-17_17_59.csv'

# Read the CSV file
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the first 20 records
df.show(20)

# Stop the Spark session when done
spark.stop()

print("Connection closed!!")

import os
from dotenv import load_dotenv # type: ignore

# loading all the env variables from .env file
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MONGO_DB_URL = os.getenv("MONGO_DB_URL")
S3_MODEL_DIR_KEY = os.getenv("S3_MODEL_DIR_KEY")
S3_MODEL_BUCKET_NAME = os.getenv("S3_MODEL_BUCKET_NAME")
MODEL_SAVED_DIR = os.getenv("MODEL_SAVED_DIR")

print(MODEL_SAVED_DIR)

print("All env variables are working!!")

