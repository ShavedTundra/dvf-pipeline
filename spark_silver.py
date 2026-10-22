from pyspark.sql import SparkSession    
from pyspark.sql import functions as F, Window

spark = SparkSession.builder \
        .appName("spark_silver") \
        .config("spark.sql.shuffle.partitions", 8) \
        .getOrCreate()

path = [f"data/bronze/true_dvf_44_{year}.csv" for year in range(2021,2026)]
bronze = spark.read.option("inferSchema", True).csv(path, header=True)

#bronze.printSchema()
#keyed = bronze.withColumn("mutation_key", functions.concat_ws(".", "date_mutation", "valeur_fonciere", "Section", "No plan"))


dedup = bronze.withColumn("rn", F.row_number().over(Window.partitionBy("id_mutation").orderBy(F.col("surface_reelle_bati").desc_nulls_last(), F.col("numero_disposition").asc())))

silver = (dedup.filter(F.col("rn")== 1)) \
    .withColumn("date_mutation", F.to_date("date_mutation")) \
    .withColumn("valeur_eur", F.col("valeur_fonciere").cast("long")) \
    .withColumn("surface_bati", F.col("surface_reelle_bati").cast("double")) \
    .withColumn("type_local", F.trim("type_local")) \
    .withColumn("code_insee", F.lpad(F.col("code_commune").cast("string"), 5, "0")) \
    .withColumn("px_m2", F.col("valeur_eur") / F.when(F.col("surface_bati") != 0.0, F.col("surface_bati"))) \
    .withColumn("annee", F.year("date_mutation"))

silver = silver.filter(F.col("nature_mutation") == "Vente") \
    .filter(F.col("type_local").isin("Appartement", "Maison")) \
    .filter(F.col("valeur_eur") > 1e4) \
    .filter(F.col("surface_bati") >= 9) \
    .drop("rn")

### 3. Assert parity
print(f"We have {silver.count()} rows in Silver table")
sum_valeur_eur = silver.select(F.sum("valeur_eur")).collect()[0][0]
print(f"The sum of valeur of goods is {sum_valeur_eur} €")
median_px_m2 = silver.select(F.round(F.median("px_m2"), 2)).collect()[0][0]
print(f"The median of px_m2 is {median_px_m2} €")
print(f"The median per commune of px_m2 is :")
median_px_m2_commune = silver.groupBy("code_insee").agg(F.round(F.median("px_m2"), 2).alias("px_m2 median per commune")).orderBy(F.col("px_m2 median per commune").desc_nulls_last())
median_px_m2_commune.show()

### 4. Write as parquet / partition by year
silver.write.partitionBy("annee").parquet("data/silver_spark/sales")