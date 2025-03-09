from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ✅ Initialisation Spark avec MinIO
spark = SparkSession.builder.appName("ProductHuntSQLAnalysis") \
    .config("spark.jars", "/work/hadoop-aws-3.3.4.jar,/work/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", "Lo2D7Zru5gN7qJNu9hZX") \
    .config("spark.hadoop.fs.s3a.secret.key", "tcwGqjIWYDU2VqSTJnIHCmSIhSRKX2ZGdhJnGkEP") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.136.127.1:9900") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

bucket_name = "bucket-21012"
s3_path = f"s3a://{bucket_name}/product-hunt.csv"

# ✅ Charger le fichier product-hunt.csv depuis MinIO
try:
    df = spark.read.option("header", "true").csv(s3_path)
    print(f"Données chargées depuis MinIO : {s3_path}")
except Exception as e:
    print(f"Erreur lors du chargement du fichier depuis MinIO: {e}")
    exit(1)

# ✅ Vérifier et convertir les types de données
df = df.withColumn("upvotes", col("upvotes").cast("int")) \
       .withColumn("product_ranking", col("product_ranking").cast("float"))

# ✅ Vérifier les types après conversion
df.printSchema()

# ✅ Créer une table temporaire Spark SQL
df.createOrReplaceTempView("product_data")

# 🟢 **Q2 : Nombre de catégories uniques (Correction)**
query_unique_categories = "SELECT COUNT(DISTINCT category_tags) AS unique_categories FROM product_data"
result_unique_categories = spark.sql(query_unique_categories)
result_unique_categories.show()

# 🟢 **Q3 : Top 5 des produits avec le plus de votes**
query_top5_upvotes = """
SELECT name, upvotes
FROM product_data
ORDER BY upvotes DESC
LIMIT 5
"""
result_top5_upvotes = spark.sql(query_top5_upvotes)
result_top5_upvotes.show()

# 🟢 Q4 (Correction) : Les 3 catégories avec le moins de produits
query_corrected_bottom3_categories = """
SELECT TRIM(category) AS category, COUNT(*) AS product_count
FROM (
    SELECT EXPLODE(SPLIT(category_tags, ',')) AS category
    FROM product_data
) 
WHERE category IS NOT NULL AND category != ''
GROUP BY category
ORDER BY product_count ASC
LIMIT 3
"""
result_corrected_bottom3_categories = spark.sql(query_corrected_bottom3_categories)
result_corrected_bottom3_categories.show()

# 🚀 **Terminer la session Spark**
spark.stop()
