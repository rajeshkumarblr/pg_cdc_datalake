import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, min, max, avg

def main():
    print("Starting Spark Analytics...")

    spark = SparkSession.builder \
        .appName("DeltaAnalytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    products_path = "data/products"
    target_path = "data/target_products"

    if not os.path.exists(products_path):
        print(f"Error: {products_path} does not exist.")
        sys.exit(1)

    print("Reading CDC Parquet files...")
    changes_df = spark.read.format("parquet").load(products_path)

    # Dedup taking latest commit version per id
    merged_df = changes_df \
        .withColumn("rank", expr("row_number() OVER (PARTITION BY id ORDER BY _commit_version DESC)")) \
        .filter("rank = 1 AND _change_type != 'delete'") \
        .drop("rank", "_change_type", "_commit_version", "_commit_timestamp")

    print("\n--- Analytics on Target Products Table ---")
    merged_df.select(
        min("price").alias("Min_Price"),
        max("price").alias("Max_Price"),
        avg("price").alias("Average_Price"),
        expr("count(1)").alias("Total_Products")
    ).show()

    print("\n--- Sample of Products ---")
    merged_df.orderBy("id").show(5)
    merged_df.orderBy(col("id").desc()).show(5)

if __name__ == "__main__":
    main()
