from typing import cast
import sys
import os 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col, desc, avg, min, max, rank, asc, round, pow, row_number
from pyspark.sql import Window
from pyspark.sql.functions import udf, struct, lit, sqrt as spark_sqrt 

spark = SparkSession.builder\
        .master("local")\
        .appName("Local_Analisis")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

####### PROGRAME SU RESPUESTA AQUI ##############

# --- CÓDIGO DE RECUPERACIÓN DEL RESULTADO (DEL EJERCICIO 5.2) ---

# 5.1 Leer el archivo JSON
ruta_json = r"C:\Users\KRSSA\Downloads\Prueba_Técnica\Procedimiento\Test\CENTROS_EDUCATIVOS_MADRID.json"

df_raw = spark.read.option("multiline", "true").json(ruta_json)

df_centros_base = df_raw.select(
col("centro_codigo").alias("codigo"),
col("centro_nombre").alias("denominacion"), 
col("centro_titularidad").alias("titularidad"),
col("direccion_coor_x").cast(DoubleType()).alias("coord_x"),
col("direccion_coor_y").cast(DoubleType()).alias("coord_y")
)

df_centros = df_centros_base.filter(col("coord_x").isNotNull() & col("coord_y").isNotNull())

# 5.2.1 Determinar el promedio (Centroide) por 'centro_titularidad'.

df_centroides = df_centros.groupBy("titularidad").agg(
avg("coord_x").alias("promedio_x"),
avg("coord_y").alias("promedio_y")
)

df_unido = df_centros.join(df_centroides, on="titularidad", how="inner")

# 5.2.2 Crear una UDF para calcular la Distancia Euclidiana

df_distancia = df_unido.withColumn(
"distancia_al_centroide",
spark_sqrt(
    pow(col("promedio_x") - col("coord_x"), 2) +
    pow(col("promedio_y") - col("coord_y"), 2)
)
)

# 5.2.3 Identificar la unidad educativa con menor distancia (el más céntrico)

window_spec = Window.partitionBy("titularidad").orderBy(col("distancia_al_centroide").asc())

df_centros_reunion = df_distancia.withColumn("rank_distancia", row_number().over(window_spec)) \
.filter(col("rank_distancia") == 1) \
.select(
col("titularidad").alias("Centro_Titularidad"),
col("denominacion").alias("Centro_Reunion"),
col("distancia_al_centroide").alias("Minima_Distancia_Euclidiana"),
col("promedio_x").alias("Coord_X_Promedio_Grupo"),
col("promedio_y").alias("Coord_Y_Promedio_Grupo")
)

# 5.3 Exportar el resultado del ejercicio anterior

# RUTA DE SALIDA LOCAL
base_path = r"C:\Users\KRSSA\Downloads\Prueba_Técnica\Procedimiento\Test\output_parquet"

# 1. Exportar a Parquet
df_centros_reunion.write.mode("overwrite").parquet(f"{base_path}_parquet")

# 2. Exportar a JSON
df_centros_reunion.write.mode("overwrite").json(f"{base_path}_json")

# 3. Exportar a CSV (separado por '|' y conservando la cabecera)
df_centros_reunion.write.mode("overwrite").option("header", "true").option("sep", "|").csv(f"{base_path}_csv")


# 5.3 Justificación:
#
# 5.4. Qué formato utilizaría para almacenar y procesar grandes volúmenes de datos en Databricks.
#
# El formato ideal para almacenar y procesar grandes volúmenes de datos en Databricks/Spark es **PARQUET**.
#
# Justificación de la elección (Parquet):
# 1. Almacenamiento Columnar: Parquet almacena los datos por columnas. Esto permite a Spark (Databricks) leer solo las columnas necesarias para una consulta, optimizando drásticamente la E/S (Input/Output). Esto es imposible con JSON o CSV, que son formatos basados en filas (row-based).
# 2. Compresión y Eficiencia: Parquet soporta esquemas de codificación y compresión optimizados para datos analíticos, lo que resulta en archivos mucho más pequeños y una ejecución de consultas más rápida.
# 3. Schema Evolution: Parquet almacena el esquema junto con los datos, asegurando la consistencia de tipos y eliminando la necesidad de inferir el esquema en cada lectura (un problema común con CSV y JSON, que son formatos auto-descriptivos o no tienen esquema).
#
# Por qué no seleccionó los otros dos formatos:
# 1. CSV (Comma Separated Values): Es el formato más ineficiente para Big Data. No tiene esquema, no está optimizado para compresión ni procesamiento paralelo eficiente, y requiere que Spark lea la fila completa incluso si solo se necesita una columna.
# 2. JSON (JavaScript Object Notation): Aunque es flexible, JSON es un formato basado en texto y en filas. Es más lento de leer que Parquet porque requiere un análisis (parsing) intensivo de texto en cada fila y no se beneficia de la poda de columnas (column pruning). Además, es más pesado en disco debido a la repetición de los nombres de los campos en cada registro.
#
#################################################

spark.stop()