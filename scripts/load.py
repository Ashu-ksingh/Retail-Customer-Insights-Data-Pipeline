# load.py – Save transformed data into PostgreSQL using psycopg2 + pandas
import os
import sys

# ✅ Add parent directory to path so config is found
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import create_engine
from config.db_config import DB_CONFIG

# Force PySpark to use Python 3.11
os.environ["PYSPARK_PYTHON"] = r"C:\Users\croma\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\croma\AppData\Local\Programs\Python\Python311\python.exe"

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)

def load_to_postgres(final_df):
    print("🔹 Starting data load to PostgreSQL...")

    # Convert Spark DataFrame to Pandas DataFrame
    pdf = final_df.toPandas()

    # Create PostgreSQL connection string
    conn_str = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # Create SQLAlchemy engine
    engine = create_engine(conn_str)

    # Load data into PostgreSQL
    try:
        pdf.to_sql("customer_insights", engine, if_exists="replace", index=False)
        print("✅ Data successfully loaded into PostgreSQL (table: customer_insights)")
    except Exception as e:
        print(f"❌ Error while loading data: {e}")

# Quick test
if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data

    o_df, c_df, p_df, r_df = extract_data()
    final_df = transform_data(o_df, c_df, p_df, r_df)
    load_to_postgres(final_df)
