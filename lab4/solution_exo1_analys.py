from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import round as spark_round


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .appName("StructuredKafkaWordCount") \
        .getOrCreate()
        
        
df_schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_name", StringType()),
    StructField("card_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("order_date", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("ecommerce_website_name", StringType())
])        
    

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-60py3.europe-west9.gcp.confluent.cloud:9092") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="FCLAEESPDSIC7UXR" password="cfltt3mqlGreq7TUTihrQURLGc89LrQPW//flojUU+1Npqh88mMXbE/OHMZvfWYQ";') \
    .option("subscribe", "ecommerce") \
    .option("startingOffsets", "earliest") \
    .load()\
         .selectExpr("CAST(value AS STRING)")\
            .select(from_json(col("value"), df_schema).alias("data"))\
            .select("data.*")
    
df_with_amount_by_countryand_city = df.groupBy("country", "city")\
    .sum("amount")\
    .withColumnRenamed("sum(amount)", "total_amount")
    
df_with_amount_by_countryand_city = df_with_amount_by_countryand_city.select(
    "country",
    "city",
    spark_round("total_amount", 2).alias("total_amount")
)

query = df_with_amount_by_countryand_city.writeStream\
    .outputMode("complete")\
    .format("console")\
    .trigger(processingTime='30 seconds')\
    .start()

query.awaitTermination()