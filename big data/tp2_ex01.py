# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     window, col, count, from_json, to_json, struct
# )
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# spark = SparkSession.builder \
#     .appName("AnalyseLogsNginx") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
#     .config("spark.ui.port", "4050") \
#     .getOrCreate()

# schema = StructType([
#     StructField("@timestamp", TimestampType(), True),
#     StructField("message", StringType(), True)
# ])

# logs = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("subscribe", "nginx-logs") \
#     .option("startingOffsets", "earliest") \
#     .load()

# parsed_logs = logs.select(from_json(col("value").cast("string"), schema).alias("data")) \
#     .select(col("data.@timestamp").alias("timestamp"), col("data.message"))

# errors_4xx = parsed_logs.filter(col("message").rlike("\\s4[0-9][0-9]\\s")) \
#     .withWatermark("timestamp", "24 hours") \
#     .groupBy(window(col("timestamp"), "24 hours")) \
#     .count()

# errors_4xx_json = errors_4xx.select(to_json(struct(col("window"), col("count"))).alias("value"))

# query = errors_4xx_json.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("topic", "21012-question-1") \
#     .outputMode("update") \
#     .option("checkpointLocation", "file:///tmp/kafka-checkpoints") \
#     .start()

# query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     window, col, count, from_json, to_json, struct, regexp_extract, desc
# )
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# spark = SparkSession.builder \
#     .appName("AnalyseLogsNginx") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
#     .config("spark.ui.port", "4050") \
#     .getOrCreate()

# log_schema = StructType([
#     StructField("@timestamp", TimestampType(), True),
#     StructField("message", StringType(), True)
# ])

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("subscribe", "nginx-logs") \
#     .option("startingOffsets", "earliest") \
#     .load()

# parsed_logs = df.select(from_json(col("value").cast("string"), log_schema).alias("data")).select("data.*")

# logs_with_ip_status = parsed_logs \
#     .withColumn("ip", regexp_extract(col("message"), r"^(\d+\.\d+\.\d+\.\d+)", 1)) \
#     .withColumn("status", regexp_extract(col("message"), r"\" (\d{3}) ", 1).cast("integer"))

# errors_4xx = logs_with_ip_status.filter((col("status") >= 400) & (col("status") < 500))

# top_10_ips = errors_4xx.groupBy("ip") \
#     .count() \
#     .orderBy(desc("count")) \
#     .limit(10)

# top_10_ips_json = top_10_ips.select(to_json(struct(col("ip"), col("count"))).alias("value"))

# query = top_10_ips_json.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("topic", "21012-question-2") \
#     .outputMode("complete") \
#     .option("checkpointLocation", "/tmp/kafka-checkpoints-q2") \
#     .start()

# query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     window, col, count, from_json, to_json, struct, regexp_extract, desc
# )
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# spark = SparkSession.builder \
#     .appName("AnalyseLogsNginx") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
#     .config("spark.ui.port", "4050") \
#     .getOrCreate()

# log_schema = StructType([
#     StructField("@timestamp", TimestampType(), True),
#     StructField("message", StringType(), True)
# ])

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("subscribe", "nginx-logs") \
#     .option("startingOffsets", "earliest") \
#     .load()

# parsed_logs = df.select(from_json(col("value").cast("string"), log_schema).alias("data")).select("data.*")

# logs_with_ip_status = parsed_logs \
#     .withColumn("ip", regexp_extract(col("message"), r"^(\d+\.\d+\.\d+\.\d+)", 1)) \
#     .withColumn("status", regexp_extract(col("message"), r"\" (\d{3}) ", 1).cast("integer"))

# parsed_logs = df.select(from_json(col("value").cast("string"), log_schema).alias("data")).select("data.*")

# logs_with_endpoint = parsed_logs.withColumn(
#     "endpoint", regexp_extract(col("message"), r"\"[A-Z]+\s(/[\w/-]*)\sHTTP", 1)
# )

# top_10_endpoints = logs_with_endpoint.groupBy("endpoint") \
#     .count() \
#     .orderBy(desc("count")) \
#     .limit(10)

# top_10_endpoints_json = top_10_endpoints.select(to_json(struct(col("endpoint"), col("count"))).alias("value"))

# query = top_10_endpoints_json.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("topic", "21012-question-3") \
#     .outputMode("complete") \
#     .option("checkpointLocation", "/tmp/kafka-checkpoints-q3") \
#     .start()

# query.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     window, col, count, from_json, to_json, struct, regexp_extract, desc
# )
# from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# spark = SparkSession.builder \
#     .appName("AnalyseLogsNginx") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
#     .config("spark.ui.port", "4050") \
#     .getOrCreate()

# log_schema = StructType([
#     StructField("@timestamp", TimestampType(), True),
#     StructField("message", StringType(), True)
# ])

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("subscribe", "nginx-logs") \
#     .option("startingOffsets", "earliest") \
#     .load()

# parsed_logs = df.select(from_json(col("value").cast("string"), log_schema).alias("data")).select("data.*")

# logs_with_method = parsed_logs.withColumn(
#     "method", regexp_extract(col("message"), r"\"([^\s]+) [^\s]+ HTTP", 1)
# )

# methods = logs_with_method.groupBy("method") \
#     .count() \
#     .orderBy(desc("count")) \
#     .limit(10)

# methods_json = methods.select(to_json(struct(col("method"), col("count"))).alias("value"))

# query = methods_json.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
#     .option("topic", "21012-question-4") \
#     .outputMode("complete") \
#     .option("checkpointLocation", "/tmp/kafka-checkpoints-q4") \
#     .start()

# query.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window, col, count, from_json, to_json, struct, regexp_extract, desc, approx_count_distinct
)

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("AnalyseLogsNginx") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

log_schema = StructType([
    StructField("@timestamp", TimestampType(), True),
    StructField("message", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
    .option("subscribe", "nginx-logs") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_logs = df.select(from_json(col("value").cast("string"), log_schema).alias("data")).select("data.*")

logs_with_ip = parsed_logs.withColumn(
    "ip", regexp_extract(col("message"), r"(\d+\.\d+\.\d+\.\d+)", 1)
)

unique_ips_count = logs_with_ip.groupBy(window(col("@timestamp"), "24 hours")).agg(
    approx_count_distinct("ip").alias("unique_ips_count")
)

unique_ips_json = unique_ips_count.select(to_json(struct(col("window"), col("unique_ips_count"))).alias("value"))

query = unique_ips_json.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.136.127.1:9092") \
    .option("topic", "21012-question-5") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/kafka-checkpoints-q5") \
    .start()

query.awaitTermination()