from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

from delta import configure_spark_with_delta_pip

def main():
    print("Starting Spark Structured Streaming CDC Analytics...")
    
    builder = SparkSession.builder \
        .appName("ECommerce_CDC_Analytics") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Disable excessive logging
    spark.sparkContext.setLogLevel("WARN")

    # Define the schema of the Parquet files written by CDC pipeline
    # The CDC pipeline prepends _cdc_operation, _cdc_lsn, and _cdc_timestamp
    order_items_schema = StructType([
        StructField("_cdc_operation", StringType(), True),
        StructField("_cdc_lsn", LongType(), True),
        StructField("_cdc_timestamp", LongType(), True),
        StructField("id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("price_at_purchase", StringType(), True)
    ])

    # Read streaming data from the delta directory
    # maxFilesPerTrigger helps throttle processing to simulate real streaming even if there's a backlog
    df = spark.readStream \
        .format("delta") \
        .option("maxFilesPerTrigger", 1) \
        .load("/home/documentdb/pg_cdc_datalake/data/order_items")

    # Filter only INSERTs (new items purchased)
    inserts = df.filter(col("_cdc_operation") == "INSERT")

    # Convert columns to appropriate types for aggregation
    cleaned_df = inserts.withColumn("product_id", col("product_id").cast("integer")) \
                        .withColumn("quantity", col("quantity").cast("integer")) \
                        .withColumn("revenue", col("quantity") * col("price_at_purchase").cast("double"))

    # Compute trending sales: total quantity and revenue per product
    trends = cleaned_df.groupBy("product_id") \
                       .agg(
                           sum("quantity").alias("total_units_sold"),
                           sum("revenue").alias("total_revenue")
                       ) \
                       .orderBy(col("total_revenue").desc())

    print("Submitting streaming query. Awaiting CDC Parquet files...")
    
    # Write the output to console continuously
    query = trends.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
