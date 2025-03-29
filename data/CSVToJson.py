from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col

# Get spark session
spark = SparkSession.builder \
    .appName("CSV to JSON Converter") \
    .getOrCreate()

# Read CSV file into a DataFrame
df = spark.read\
    .option("header", True)\
    .option("multiline", True)\
    .option("inferSchema", True)\
    .option("sep", ",")\
    .option("quote", "\"").option("escape", "\"")\
    .csv("./data/hashtag_joebiden.csv")

# Remove all , characters
df = df.select([
    regexp_replace(col(c), ",", " ").alias(c) if dtype == "string" else col(c)
    for c, dtype in df.dtypes
])

df.printSchema()
df.select("created_at", "tweet_id").show(15, truncate=False)

# Write the DataFrame to a JSON file 
#df.coalesce(1).write.mode("overwrite").json("output")