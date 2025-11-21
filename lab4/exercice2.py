from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, round, when, regexp_replace, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import sum as _sum


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("CSVStreamReader") \
        .getOrCreate()
        
# Schema avec original_price en StringType pour gérer les valeurs manquantes et format avec $
schema = StructType([
    StructField("url", StringType(), True),
    StructField("name", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("selling_price", DoubleType(), True),
    StructField("original_price", StringType(), True),  # StringType pour capturer les valeurs manquantes et $ format
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

# Nettoyage : convertir original_price de string à double
# Gérer les cas : valeurs manquantes (""), format avec $ ("$25"), et nulls
df_clean = df.withColumn(
    "original_price_clean",
    when(
        (col("original_price").isNull()) | (col("original_price") == ""),
        None  # Valeur manquante
    ).otherwise(
        # Supprimer les symboles $ et convertir en double
        regexp_replace(col("original_price"), "[$,]", "").cast(DoubleType())
    )
)

# Afficher les données pour débogage (optionnel)
# df_clean.select("name", "selling_price", "original_price", "original_price_clean").show()
    

    
#  top 5 products having highest reviews and least expensive in unit price
agg_reviews = df_clean.groupBy("url", "name", "selling_price")\
    .agg(_sum("reviews_count").alias("total_reviews"))

top5_popular_cheap = agg_reviews.orderBy(
    col("total_reviews").desc(),
    col("selling_price").asc()
).limit(5)


# top 5 products having the biggest percentage of discount (selling price vs original price)
# IMPORTANT : Filtrer les produits sans original_price pour éviter les calculs incorrects
df_discount = df_clean.filter(col("original_price_clean").isNotNull()).withColumn(
    "discount_percentage",
    when(
        col("original_price_clean") > 0,  # Éviter division par zéro
        round((col("original_price_clean") - col("selling_price")) / col("original_price_clean") * 100, 2)
    ).otherwise(0)  # Si original_price est 0 ou négatif, discount = 0
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
        
    
