from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import sum as _sum


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("CSVStreamReader") \
        .getOrCreate()
        
schema = StructType([
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("selling_price", DoubleType(), True),
    StructField("original_price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("availability", StringType(), True),
    StructField("color", StringType(), True),
    StructField("category", StringType(), True),
    StructField("source", StringType(), True),
    StructField("source_website", StringType(), True),
    StructField("breadcrumbs", StringType(), True),
    StructField("description", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("images", StringType(), True),
    StructField("country", StringType(), True),
    StructField("language", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("reviews_count", IntegerType(), True),
    StructField("crawled_at", TimestampType(), True)
])
        
df = spark.readStream.format("csv")\
    .option("header", "true") \
    .schema(schema)\
    .load("./data/adidas/stream/")
    

    
#  top 5 products having highest reviews and least expensive in unit price
agg_reviews = df.groupBy("url", "name", "selling_price")\
    .agg(_sum("reviews_count").alias("total_reviews"))

top5_popular_cheap = agg_reviews.orderBy(
    col("total_reviews").desc(),
    col("selling_price").asc()
).limit(5)


# top 5 products having the biggest percentage of discount (selling price vs original price)
df_discount = df.withColumn(
    "discount_percentage",
    round((col("original_price") - col("selling_price")) / col("original_price") * 100, 2)
)

agg_discount = df_discount.groupBy("url", "name")\
    .agg(_sum("discount_percentage").alias("total_discount"))

top5_discount = agg_discount.orderBy(col("total_discount").desc()).limit(5)


    
# query = df.writeStream\
#     .outputMode("append")\
#     .format("console")\
#     .start()    
    
# query.awaitTermination()

console1 = top5_popular_cheap.writeStream\
    .outputMode("complete")\
    .format("console")\
        .trigger(processingTime='30 seconds')\
    .start()
    
console2 = top5_discount.writeStream\
    .outputMode("complete")\
    .format("console")\
        .trigger(processingTime='30 seconds')\
    .start()
    
console1.awaitTermination()
console2.awaitTermination()
        
    
