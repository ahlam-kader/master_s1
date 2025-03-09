from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, element_at
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import (
    NGram,
    CountVectorizer,
    StringIndexer,
    IndexToString,
    IDF
)
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array

spark = SparkSession.builder.appName("GenderPredictionImprovements").getOrCreate()

def to_char_list(name):
    return list(name.strip().lower()) if name else []

char_list_udf = udf(to_char_list, ArrayType(StringType()))

def merge_arrays(arr1, arr2, arr3):
    if arr1 is None: arr1 = []
    if arr2 is None: arr2 = []
    if arr3 is None: arr3 = []
    return arr1 + arr2 + arr3

merge_arrays_udf = udf(merge_arrays, ArrayType(StringType()))

data = spark.read.csv("names-mr.csv", header=True, inferSchema=True, sep=";")
data = data.withColumnRenamed("NOMPL", "name").withColumnRenamed("SEXE", "gender")
data = data.withColumn("chars", char_list_udf(col("name")))

ngram2 = NGram(inputCol="chars", outputCol="bigrams", n=2)
ngram3 = NGram(inputCol="chars", outputCol="trigrams", n=3)
data = ngram2.transform(data)
data = ngram3.transform(data)

data = data.withColumn("combined", merge_arrays_udf(col("chars"), col("bigrams"), col("trigrams")))

labelIndexer = StringIndexer(inputCol="gender", outputCol="label")
cv = CountVectorizer(inputCol="combined", outputCol="rawFeatures", vocabSize=5000, minDF=1)

idf = IDF(inputCol="rawFeatures", outputCol="features")
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    probabilityCol="probability",
    regParam=0.1,
    maxIter=50
)
pipeline_lr = Pipeline(stages=[labelIndexer, cv, idf, lr])
model_lr = pipeline_lr.fit(data)
lr_labels = model_lr.stages[0].labels
lr_converter = IndexToString(inputCol="prediction", outputCol="predictedGender", labels=lr_labels)

nb = NaiveBayes(
    featuresCol="rawFeatures",
    labelCol="label",
    probabilityCol="probability",
    smoothing=2.0
)
pipeline_nb = Pipeline(stages=[labelIndexer, cv, nb])
model_nb = pipeline_nb.fit(data)
nb_labels = model_nb.stages[0].labels
nb_converter = IndexToString(inputCol="prediction", outputCol="predictedGender", labels=nb_labels)

students = spark.read.csv("deml.csv", header=True, inferSchema=True, sep=";")
students = students.withColumnRenamed("NOMPL", "name").withColumnRenamed("SEXE", "gender")
students = students.withColumn("chars", char_list_udf(col("name")))
students = ngram2.transform(students)
students = ngram3.transform(students)
students = students.withColumn("combined", merge_arrays_udf(col("chars"), col("bigrams"), col("trigrams")))

students_predictions_lr = model_lr.transform(students)
students_predictions_lr = lr_converter.transform(students_predictions_lr)
students_predictions_lr = students_predictions_lr.withColumn(
    "Probability",
    element_at(vector_to_array(col("probability")), (col("prediction").cast("int") + 1))
)
output_lr = students_predictions_lr.select("name", "predictedGender", "Probability")
output_lr.show(truncate=False)
output_lr.write.csv("students_predictions_lr.csv", header=True)

students_predictions_nb = model_nb.transform(students)
students_predictions_nb = nb_converter.transform(students_predictions_nb)
students_predictions_nb = students_predictions_nb.withColumn(
    "Probability",
    element_at(vector_to_array(col("probability")), (col("prediction").cast("int") + 1))
)
output_nb = students_predictions_nb.select("name", "predictedGender", "Probability")
output_nb.show(truncate=False)
output_nb.write.csv("students_predictions_nb.csv", header=True)

true_values = spark.read.csv("deml.csv", header=True, inferSchema=True, sep=";")

joined_lr = output_lr.join(true_values.select("name", "gender"), on="name", how="inner")
accuracy_lr = joined_lr.filter(col("predictedGender") == col("gender")).count() / joined_lr.count()

joined_nb = output_nb.join(true_values.select("name", "gender"), on="name", how="inner")
accuracy_nb = joined_nb.filter(col("predictedGender") == col("gender")).count() / joined_nb.count()

accuracy_data = [("Logistic Regression", accuracy_lr), ("Naive Bayes", accuracy_nb)]
accuracy_df = spark.createDataFrame(accuracy_data, ["Model", "Accuracy"])

accuracy_df.show(truncate=False)

spark.stop()
