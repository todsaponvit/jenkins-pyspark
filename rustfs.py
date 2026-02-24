from pyspark.sql import SparkSession
from pyspark import SparkFiles


spark = SparkSession.builder \
            .appName("PySpark+RUSTFS") \
            .getOrCreate()

BUCKET = 'pyspark'

df = spark.read.csv(f"s3a://{BUCKET}/zipcodes.csv", header=True, inferSchema=True)
df.show(5)

df.printSchema()

spark.stop()
