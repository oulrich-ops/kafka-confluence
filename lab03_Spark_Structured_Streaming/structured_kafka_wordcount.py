from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .appName("StructuredKafkaWordCount") \
        .getOrCreate()
        
        
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
    .load()

words = df.selectExpr("CAST(value AS STRING) as line")

words_exploded = words.select(explode(split(words.line, " ")).alias("word"))

word_counts = words_exploded.groupBy("word").count()

query = word_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime='10 seconds')\
    .start()

query.awaitTermination()