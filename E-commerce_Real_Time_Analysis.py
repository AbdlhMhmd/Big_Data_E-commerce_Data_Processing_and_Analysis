from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import DataFrame
import logging

# configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# Initialize Spark Session with appropriate configurations for Ecommerce Data Analysis
spark = SparkSession.builder \
    .appName("Ecommerce Data Analysis") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Kafka bootstrap servers configuration
kafka_bootstrap_servers = "localhost:9092"

#Read Topics/////////////////////////

# Read data from 'ecommerce_customers' topic
customerSchema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("account_created", StringType(), True),
    StructField("last_login", TimestampType(), True) 
])
customerDF = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
              .option("subscribe", "ecommerce_customers")
              .option("startingOffsets", "earliest")  # Start from the earliest records
              .load()
              .selectExpr("CAST(value AS STRING)")
              .select(from_json("value", customerSchema).alias("data"))
              .select("data.*")
              .withWatermark("last_login", "2 hours") 
            
             )


# Read data from 'ecommerce_products' topic
productSchema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("supplier", StringType(), True),
    StructField("rating", DoubleType(), True),
    StructField("last_buy", TimestampType(), True) 
])
productDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_products") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", productSchema).alias("data")) \
    .select("data.*") \
    .withWatermark("last_buy", "2 hours")
    
                               



# Read data from 'ecommerce_transactions' topic
transactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),  
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("last_trans", TimestampType(), True) 
])
transactionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_transactions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transactionSchema).alias("data")) \
    .select("data.*")
  
       




#start queries/////////////////////////

            
#1 Customer by gender.
customerAnalysisDF = ( customerDF
                      .groupBy(
                           window(col("last_login"), "1 day"),  # Windowing based on last_login
                          when(col("gender") == "Female", "Female")
                          .when(col("gender") == "Male", "Male")
                          .otherwise("Other")
                          .alias("gender_category")
                      )
                      .agg(
                          count("customer_id").alias("total_customers")
                      )
                      .filter("gender_category IN ('Female', 'Male')")
                 
                     )
                     


# 2- Product with windowing in one
productAnalysisDF = productDF \
    .groupBy(
       window(col("last_buy"), "1 day")
    ) \
    .agg(
        avg("price").alias("average_price"),
        sum("stock_quantity").alias("total_stock")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
  
        col("average_price"),
        col("total_stock")
    )\
         .writeStream \
              .outputMode("append") \
              .format("console") \
              .start() \
              .awaitTermination()  
    
   

#3- Analyzing sales data
salesAnalysisDF = transactionDF \
    .groupBy(
        window(col("date_time"), "1 hour"),  # Window based on processingTime
        "product_id"
    ) \
    .agg(
        count("transaction_id").alias("number_of_sales"),
        sum("quantity").alias("total_quantity_sold"),
        approx_count_distinct("customer_id").alias("unique_customers")  # Use approx_count_distinct
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("product_id"),
        col("number_of_sales"),
        col("total_quantity_sold"),
        col("unique_customers")
    )\
   




# Write the streaming data to HDFS in Parquet format

#Customer by Age
query2 = (productViewsAnalysisDF.writeStream 
    .outputMode("append")  # or 'complete' or 'update' based on your use case
    .format("parquet")
    .option("path", "hdfs://localhost:9000/query2")  # Specify your HDFS path
    .option("checkpointLocation", "hdfs://localhost:9000/query2Checkpoint")  # Specify your checkpoint path
    .start())
# Wait for the streaming query to finish
query2.awaitTermination()


#Customer by Age
query1 = (customerAnalysisDF.writeStream 
    .outputMode("append")  # or 'complete' or 'update' based on your use case
    .format("parquet")
    .option("path", "hdfs://localhost:9000/query1")  # Specify your HDFS path
    .option("checkpointLocation", "hdfs://localhost:9000/query1Checkpoint")  # Specify your checkpoint path
    .start())
# Wait for the streaming query to finish
query1.awaitTermination()



#Customer by Age
query3 = (salesAnalysisDF.writeStream 
    .outputMode("append")  # or 'complete' or 'update' based on your use case
    .format("parquet")
    .option("path", "hdfs://localhost:9000/query3")  # Specify your HDFS path
    .option("checkpointLocation", "hdfs://localhost:9000/query3Checkpoint")  # Specify your checkpoint path
    .start())
# Wait for the streaming query to finish
query3.awaitTermination()
