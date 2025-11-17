from typing import cast
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col, desc, avg, min, max, rank, asc, round
from pyspark.sql import Window
from math import sqrt
from pyspark.sql.functions import udf, struct, lit, row_number, pow, sqrt

spark = (SparkSession.builder
.master("local")
.appName("Colab")
.config('spark.ui.port', '4050')
.getOrCreate())

####### PROGRAME SU RESPUESTA AQUI ##############

# 5.1 Leer el archivo JSON

ruta_json = "C:/Users/KRSSA/Downloads/Prueba_Técnica/Material/Test/CENTROS_EDUCATIVOS_MADRID.json"

df_raw = spark.read.option("multiline", "true").json(ruta_json)

df_centros_base = df_raw.select(
col("centro_codigo").alias("codigo"),
col("centro_nombre").alias("denominacion"), 
col("centro_titularidad").alias("titularidad"),
col("direccion_coor_x").cast(DoubleType()).alias("coord_x"),
col("direccion_coor_y").cast(DoubleType()).alias("coord_y")
)

# Filtrar centros con coordenadas válidas para asegurar el cálculo de distancia
df_centros = df_centros_base.filter(col("coord_x").isNotNull() & col("coord_y").isNotNull())

# 5.2.1 Determinar el promedio (Centroide) por 'centro_titularidad'.

df_centroides = df_centros.groupBy("titularidad").agg(
avg("coord_x").alias("promedio_x"),
avg("coord_y").alias("promedio_y")
)

df_unido = df_centros.join(df_centroides, on="titularidad", how="inner")

# 5.2.2 Crear una UDF para calcular la Distancia Euclidiana
# (Sustituido por funciones nativas de Spark por rendimiento)

# Aplicar la UDF al DataFrame
df_distancia = df_unido.withColumn(
"distancia_al_centroide",
sqrt(
    pow(col("promedio_x") - col("coord_x"), 2) +
    pow(col("promedio_y") - col("coord_y"), 2)
)
)

# 5.2.3 Identificar la unidad educativa con menor distancia (el más céntrico)
# Definir la función de ventana: particionar por titularidad y ordenar por distancia (ascendente)

window_spec = Window.partitionBy("titularidad").orderBy(col("distancia_al_centroide").asc())

# Aplicar la función de ventana para obtener el centro con menor distancia (rank 1)

df_centros_reunion = df_distancia.withColumn("rank_distancia", row_number().over(window_spec)) \
.filter(col("rank_distancia") == 1) \
.select(
col("titularidad").alias("Centro_Titularidad"),
col("denominacion").alias("Centro_Reunion"),
col("distancia_al_centroide").alias("Minima_Distancia_Euclidiana"),
col("promedio_x").alias("Coord_X_Promedio_Grupo"),
col("promedio_y").alias("Coord_Y_Promedio_Grupo")
)

# Mostrar el resultado final

df_centros_reunion.show(truncate=False)

#################################################

spark.stop()