# Import
from pyspark.sql import SparkSession
from pyspark import SparkFiles


# Create SparkSession
spark = SparkSession.builder.master("local[*]")\
          .appName("Read CSV from Websites")\
          .getOrCreate()

csv_path = "https://raw.githubusercontent.com/spark-examples/pyspark-examples/refs/heads/master/resources/zipcodes.csv"
sc = spark.sparkContext
sc.addFile(csv_path)

# Read CSV File
local_path = SparkFiles.get("zipcodes.csv")
df = spark.read.csv(local_path, header=True, inferSchema=True)

df.show(5)
df.printSchema()

spark.stop()
