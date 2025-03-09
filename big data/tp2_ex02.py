from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, count, avg, sum

spark = SparkSession.builder.master("local").appName("Analyse des Accidents de la Route").getOrCreate()
accidents_df = spark.read.option("header", True).option("inferSchema", True).csv("Global_Traffic_Accidents.csv")
accidents_df.printSchema()
accidents_par_pays = accidents_df.groupBy("Country").count().withColumnRenamed("count", "Total_Accidents")
print("Nombre d'accidents enregistrés par pays :")
accidents_par_pays.show()

accidents_par_gravite = accidents_df.groupBy("Severity").count().withColumnRenamed("count", "Nombre_Accidents")
print("Répartition des accidents en fonction de leur gravité :")
accidents_par_gravite.show()

moyenne_victimes_par_route = accidents_df.groupBy("Road_Type").agg(avg("Number_of_Casualties").alias("Moyenne_Victimes"))
print("Nombre moyen de victimes selon le type de route :")
moyenne_victimes_par_route.show()

accidents_par_heure = accidents_df.withColumn("Heure", substring("Time", 1, 2)) \
    .groupBy("Heure").agg(count("*").alias("Total_Accidents")) \
    .orderBy(col("Total_Accidents").desc())
print("Heure où les accidents sont les plus fréquents :")
accidents_par_heure.show(1)

conditions_meteo_defavorables = ["Snowy", "Foggy", "Rainy", "Stormy"]
impact_meteo = accidents_df.filter(col("Weather_Condition").isin(conditions_meteo_defavorables)) \
    .groupBy("Weather_Condition").agg(count("*").alias("Nombre_Accidents"))
print("Nombre d'accidents en fonction des conditions météorologiques difficiles :")
impact_meteo.show()

villes_plus_victimes = accidents_df.groupBy("City").agg(sum("Number_of_Casualties").alias("Total_Victimes")) \
    .orderBy(col("Total_Victimes").desc()).limit(5)
print("Top 5 des villes avec le plus grand nombre de victimes :")
villes_plus_victimes.show()

accidents_rdd = accidents_df.rdd
accidents_par_pays_rdd = accidents_rdd.map(lambda row: (row["Country"], 1)) \
                                     .reduceByKey(lambda a, b: a + b)

print("Nombre total d'accidents par pays (via RDD) :")
for pays, total in accidents_par_pays_rdd.collect():
    print(f"{pays}: {total}")

victimes_par_route_rdd = accidents_rdd.map(lambda row: (row["Road_Type"], (row["Number_of_Casualties"], 1))) \
                                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                                      .mapValues(lambda x: x[0] / x[1])  # Calcul de la moyenne

print("Nombre moyen de victimes par type de route (via RDD) :")
for route, moyenne in victimes_par_route_rdd.collect():
    print(f"{route}: {moyenne:.2f}")

spark.stop()
