# transform.py - Clean, join, and aggregate data in PySpark
from pyspark.sql import functions as F

def transform_data(orders_df, customers_df, products_df, reviews_df):
    print("🔹 Starting data transformation...")

    # --- Step 1: Data Cleaning ---
    orders_df = orders_df.dropna().dropDuplicates()
    customers_df = customers_df.dropna().dropDuplicates()
    products_df = products_df.dropna().dropDuplicates()
    reviews_df = reviews_df.dropna().dropDuplicates()

    # --- Step 2: Convert numeric columns to correct types ---
    orders_df = orders_df.withColumn("quantity", orders_df["quantity"].cast("int"))
    orders_df = orders_df.withColumn("total_amount", orders_df["total_amount"].cast("float"))
    reviews_df = reviews_df.withColumn("rating", reviews_df["rating"].cast("float"))

    # --- Step 3: Rename duplicate columns to avoid ambiguity ---
    orders_df = orders_df.withColumnRenamed("city", "order_city")
    customers_df = customers_df.withColumnRenamed("city", "customer_city")

    # --- Step 4: Join Datasets ---
    combined_df = (
        orders_df
        .join(customers_df, on="customer_id", how="left")
        .join(products_df, on="product_id", how="left")
        .join(reviews_df, on=["customer_id", "product_id"], how="left")
    )

    # --- Step 5: Derive Insights ---
    insights_df = (
        combined_df.groupBy("customer_id", "name", "customer_city")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.countDistinct("order_id").alias("total_orders"),
            F.avg("rating").alias("avg_rating")
        )
        .orderBy(F.desc("total_spent"))
    )

    print("✅ Data transformation completed.")
    return insights_df


# For quick test
if __name__ == "__main__":
    from extract import extract_data
    orders_df, customers_df, products_df, reviews_df = extract_data()
    final_df = transform_data(orders_df, customers_df, products_df, reviews_df)
    final_df.show(5)
