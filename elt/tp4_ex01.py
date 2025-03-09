from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_extract, window

KAFKA_BROKER = "10.136.127.1:9092"
KAFKA_TOPIC  = "nginx-logs"

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING) as log")

df.printSchema()  
