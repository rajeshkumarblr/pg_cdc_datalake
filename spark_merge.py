from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os

def main():
    print("Starting Spark MERGE Example...")
    
    builder = SparkSession.builder \
        .appName("ECommerce_CDC_Merge") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    try:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        print("Delta Lake not found. Please install: pip install pyspark delta-spark")
        return

    spark.sparkContext.setLogLevel("WARN")

    source_path = "/home/documentdb/pg_cdc_datalake/data/orders"
    target_path = "/home/documentdb/pg_cdc_datalake/data/target_orders"

    if not os.path.exists(source_path):
        print(f"Source path {source_path} does not exist. Please run CDC pipeline first.")
        return

    # 1. Read the CDC data
    cdc_df = spark.read.format("delta").load(source_path)

    # 2. Deduplicate: if multiple updates exist for the same row, take the latest one (highest _commit_version)
    window_spec = Window.partitionBy("id").orderBy(col("_commit_version").desc())
    latest_cdc_df = cdc_df.withColumn("rn", row_number().over(window_spec)) \
                          .filter(col("rn") == 1) \
                          .drop("rn")

    # 3. Handle Target Table Initialization
    if not DeltaTable.isDeltaTable(spark, target_path):
        print("Target table does not exist. Initializing...")
        # For the very first load, we can just take the inserts to create the target
        initial_df = latest_cdc_df.filter(col("_change_type") != "delete") \
                                  .drop("_change_type", "_commit_version", "_commit_timestamp")
        initial_df.write.format("delta").mode("overwrite").save(target_path)
        print("Initialized target table.")
    else:
        print("Merging into existing target table...")
        target_table = DeltaTable.forPath(spark, target_path)

        # Build MERGE condition
        # Match on primary key (id)
        target_table.alias("t").merge(
            latest_cdc_df.alias("s"),
            "t.id = s.id"
        ).whenMatchedUpdateAll(
            condition="s._change_type = 'update_postimage'"
        ).whenMatchedDelete(
            condition="s._change_type = 'delete'"
        ).whenNotMatchedInsertAll(
            condition="s._change_type = 'insert' OR s._change_type = 'update_postimage'"
        ).execute()
        print("Merge successful.")

    # 4. Display the resulting table
    print("\n--- Current State of Orders Table ---")
    final_df = spark.read.format("delta").load(target_path)
    final_df.orderBy("id").show(truncate=False)

if __name__ == "__main__":
    main()
