from pyspark.sql import SparkSession

bucket_name = "bucket-21012"

spark = SparkSession.builder.appName("Spark") \
    .config("spark.jars", "/work/hadoop-aws-3.3.4.jar,/work/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "AiGOMblU9UuA26XbFpQB") \
    .config("spark.hadoop.fs.s3a.secret.key", "e8s6iUWq6cZNAi7CL5FQ4DUKhuCVrt4wphSOn6eG") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Chargement du fichier dans un RDD
file = f"s3a://{bucket_name}/titanic.csv"
rdd = spark.sparkContext.textFile(file)

print(rdd.take(5))  # Vérification du chargement
