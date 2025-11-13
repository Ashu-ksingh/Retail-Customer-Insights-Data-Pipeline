# extract.py - Extract data from CSV, JSON, and MongoDB
import os
os.environ["PYSPARK_PYTHON"] = r"C:\Users\croma\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\croma\AppData\Local\Programs\Python\Python311\python.exe"

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pymongo import MongoClient
import json


# Initialize Spark session
def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("RetailCustomerInsights_Extract")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        .getOrCreate()
    )
    return spark


def extract_data():
    print("🔹 Starting data extraction...")

    spark = get_spark_session()

    # Define paths
    base_path = os.path.join(os.path.dirname(__file__), "../data")

    # --- Read CSV files ---
    orders_df = spark.read.option("header", True).csv(f"{base_path}/orders.csv")
    customers_df = spark.read.option("header", True).csv(f"{base_path}/customers.csv")

    # --- Read Product JSON (mock API) ---
    products_df = spark.read.option("multiline", True).json(f"{base_path}/products.json")

    # --- Read MongoDB collection (customer reviews) ---
    # For now, simulate MongoDB data (until MongoDB is configured)
    # You can later replace this with live connection
    reviews_data = [
        {"customer_id": 1, "product_id": "P001", "rating": 4, "review": "Good quality"},
        {"customer_id": 2, "product_id": "P002", "rating": 5, "review": "Very useful"},
        {"customer_id": 3, "product_id": "P003", "rating": 3, "review": "Average"},
        {"customer_id": 4, "product_id": "P001", "rating": 4, "review": "Works fine"}
    ]
    reviews_df = spark.createDataFrame(reviews_data)

    print("✅ Data extraction complete.")
    return orders_df, customers_df, products_df, reviews_df


# For quick testing
if __name__ == "__main__":
    o_df, c_df, p_df, r_df = extract_data()
    print("Orders Sample:")
    o_df.show(3)
    print("Customers Sample:")
    c_df.show(3)
    print("Products Sample:")
    p_df.show(3)
    print("Reviews Sample:")
    r_df.show(3)
