from pyspark.sql import SparkSession

access_key = "vcV8AL40Y3pNlrzX2su9"
secret_key = "mRhks6JF0KlgL8VzeitPWGQBOry7owIuN9Ma3C52"

spark = SparkSession.builder\
        .master("local[*]")\
        .appName("PySpark Read MySQL")\
        .getOrCreate()


        # .config('spark.jars.packages', 'com.mysql:mysql-connector-j:8.4.0')\
        # .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000/") \
        # .config("spark.hadoop.fs.s3a.access.key", access_key) \
        # .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        # .getOrCreate()

# MySQL Connection
MYSQL_HOST = '34.136.184.58'
MYSQL_PORT = '3306'
MYSQL_USER = 'r2de2'
MYSQL_PASSWORD = 'I_Love_Data_Engineer'
MYSQL_DB = 'r2de2'

# Read Table
audible_data = spark.read\
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
                .option("driver","com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "audible_data") \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASSWORD)\
                .load()

audible_data.show(5)

audible_transaction = spark.read\
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
                .option("driver","com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "audible_transaction") \
                .option("user", MYSQL_USER) \
                .option("password", MYSQL_PASSWORD)\
                .load()


# Write to RustFS
BUCKET = 'pyspark/pyspark_scipt'

audible_data.write.mode("overwrite").parquet(f"s3a://{BUCKET}/audible_data/parquet")
audible_data.write.mode("overwrite").csv(f"s3a://{BUCKET}/audible_data/csv", header=True)

audible_transaction.write.mode("overwrite").parquet(f"s3a://{BUCKET}/audible_transaction/parquet")
audible_transaction.write.mode("overwrite").csv(f"s3a://{BUCKET}/audible_transaction/csv", header=True)

print("="*50)
print("Write to RustFS Successfully")
print("="*50)

spark.stop()
