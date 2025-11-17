# Ejercicio 8 Pyspark

import sys
from pyspark.mllib.random import RandomRDDs
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, trim, desc, format_number, collect_list, struct, lit


spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

# esquema de jugador: [(ranking, nombre, codigo_pais, numero_goles, numero_partidos_jugados)]
jugador = [("1", "Cristiano Ronaldo", "32", "115", "184"),("2", "Ali Daei", "24", "109", "148"),("3", "Mokhtar Dahari", "28", "89", "142"),("4", "Ferenc Puskás", "20", "84", "89"),("5", "Lionel Messi", "5", "81", "158"),("6", "Sunil Chhetri", "21", "80", "125"),("7", "Ali Mabkhout", "13", "79", "104"),("8", "Godfrey Chitalu", "39", "79", "111"),("9", "Hussein Saeed", "23", "78", "137"),("10", "Pelé", "8", "77", "92"),("11", "Sándor Kocsis", "19", "75", "68"),("12", "Kunishige Kamamoto", "26", "75", "76"),("13", "Bashar Abdullah", "27", "75", "134"),("14", "Robert Lewandowski", "31", "74", "128"),("15", "Majed Abdullah", "4", "72", "117"),("16", "Kinnah Phiri", "29", "71", "117"),("17", "Kiatisuk Senamuang", "36", "71", "134"),("18", "Miroslav Klose", "1", "71", "137"),("19", "Piyapong Pue-on", "36", "70", "100"),("20", "Abdul Kadir", "22", "70", "111"),("21", "Stern John", "37", "70", "115"),("22", "Neymar", "8", "70", "116"),("23", "Gerd Müller", "2", "68", "62"),("24", "Romelu Lukaku", "6", "68", "101"),("25", "Carlos Ruiz Gutiérrez", "17", "68", "133"),("26", "Robbie Keane", "25", "68", "146"),("27", "Hossam Hassan", "12", "68", "176"),("28", "Luis Suárez", "38", "67", "130"),("29", "Didier Drogba", "11", "65", "105"),("30", "Jasem Al-Huwaidi", "27", "63", "83"),("31", "Ronaldo Nazario", "8", "62", "98"),("32", "Zlatan Ibrahimović", "35", "62", "120"),("33", "Ahmed Radhi", "23", "62", "121"),("34", "Abdul Ghani Minhat", "16", "61", "71"),("35", "Edin Džeko", "7", "60", "118"),("36", "Imre Schlosser", "19", "59", "68"),("37", "David Villa", "14", "59", "98"),("38", "Cha Bum-Kun", "10", "58", "135"),("39", "Ali Ashfaq", "30", "57", "89"),("40", "Carlos Pavón", "18", "57", "101"),("41", "Clint Dempsey", "15", "57", "141"),("42", "Younis Mahmoud", "23", "57", "148"),("43", "Landon Donovan", "15", "57", "157"),("44", "Samuel Eto'o", "9", "56", "118"),("45", "Romário", "8", "55", "70"),("46", "Kazuyoshi Miura", "26", "55", "89"),("47", "Jan Koller", "33", "55", "91"),("48", "Iswadi Idris", "22", "55", "97"),("49", "Fandi Ahmad", "34", "55", "101"),("50", "Joachim Streich", "3", "55", "102")]

# esquema de país: [(codigo_pais, nombre_pais)]
pais = [("1", " Alemania"),("2", " Alemania Federal"),("3", " Alemania Oriental"),("4", " Arabia Saudita"),("5", " Argentina"),("6", " Bélgica"),("7", " Bosnia y Herzegovina"),("8", " Brasil"),("9", " Camerún"),("10", " Corea del Sur"),("11", " Costa de Marfil"),("12", " Egipto"),("13", " Emiratos Arabes Unidos"),("14", " España"),("15", " Estados Unidos"),("16", " Federación Malaya/ Malasia"),("17", " Guatemala"),("18", " Honduras"),("19", " Hungría"),("20", " Hungría/ España"),("21", " India"),("22", " Indonesia"),("23", " Irak"),("24", " Irán"),("25", " Irlanda"),("26", " Japón"),("27", " Kuwait"),("28", " Malasia"),("29", " Malawi"),("30", " Maldivas"),("31", " Polonia"),("32", " Portugal"),("33", " República Checa"),("34", " Singapur"),("35", " Suecia"),("36", " Tailandia"),("37", " Trinidad y Tobago"),("38", " Uruguay"),("39", " Zambia")]

####### PROGRAME SU RESPUESTA AQUI ##############

# Respuesta 4.1
# ===============================================
jugador_schema = StructType([
    StructField("ranking_str", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("codigo_pais", StringType(), True),
    StructField("numero_goles_str", StringType(), True),
    StructField("numero_partidos_jugados_str", StringType(), True)
])

pais_schema = StructType([
    StructField("codigo_pais", StringType(), True),
    StructField("nombre_pais_sucio", StringType(), True)
])

df_jugador_raw = spark.createDataFrame(data=jugador, schema=jugador_schema)
df_pais_raw = spark.createDataFrame(data=pais, schema=pais_schema)

print("="*30)
print("Respuesta 4.1: Esquemas de los DataFrames crudos")
df_jugador_raw.printSchema()
df_pais_raw.printSchema()
print("="*30)

# Respuesta 4.2
# ===============================================
df_pais = df_pais_raw.withColumn("nombre_pais", trim(col("nombre_pais_sucio"))).drop("nombre_pais_sucio")

df_jugador = df_jugador_raw.withColumn("ranking", col("ranking_str").cast(IntegerType())) \
    .withColumn("numero_goles", col("numero_goles_str").cast(IntegerType())) \
    .withColumn("numero_partidos_jugados", col("numero_partidos_jugados_str").cast(IntegerType())) \
    .drop("ranking_str", "numero_goles_str", "numero_partidos_jugados_str")

print("Respuesta 4.2: Esquema del DataFrame de jugadores limpio")
df_jugador.printSchema()
print("\nDataFrame de países limpio (Top 5)")
df_pais.show(5, truncate=False)
print("="*30)

# Respuesta 4.3
# ===============================================
df_completo = df_jugador.join(
    df_pais,
    on="codigo_pais",
    how="left"
)

print("Respuesta 4.3: Join de Jugadores y Países (Top 10)")
df_completo.select("ranking", "nombre", "nombre_pais", "numero_goles").show(10, truncate=False)
print("="*30)

# Respuesta 4.4
# ===============================================
df_promedio = df_completo.withColumn(
    "goles_por_partido",
    col("numero_goles") / col("numero_partidos_jugados")
)

df_promedio_ordenado = df_promedio.orderBy(desc("goles_por_partido"))

print("Respuesta 4.4: Top 10 de jugadores por promedio de goles por partido")
df_promedio_ordenado.select(
    "ranking",
    "nombre",
    "nombre_pais",
    "numero_goles",
    "numero_partidos_jugados",
    format_number("goles_por_partido", 3).alias("promedio_goles")
).show(10, truncate=False)
print("="*30)

#################################################

spark.stop()