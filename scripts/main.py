# main.py – Orchestrates full ETL pipeline
import os
import sys

# ✅ Ensure project root is on path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ✅ Force PySpark to use Python 3.11
os.environ["PYSPARK_PYTHON"] = r"C:\Users\croma\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\croma\AppData\Local\Programs\Python\Python311\python.exe"

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_to_postgres


def main():
    print("🚀 Starting Retail Customer Insights ETL Pipeline...\n")

    # Extract
    print("🔹 Step 1: Extracting data...")
    orders_df, customers_df, products_df, reviews_df = extract_data()
    print("✅ Extraction complete.\n")

    # Transform
    print("🔹 Step 2: Transforming data...")
    insights_df = transform_data(orders_df, customers_df, products_df, reviews_df)
    print("✅ Transformation complete.\n")

    # Load
    print("🔹 Step 3: Loading data to PostgreSQL...")
    load_to_postgres(insights_df)
    print("\n✅ ETL Pipeline completed successfully! 🚀")


if __name__ == "__main__":
    main()
