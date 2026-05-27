from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, trim, upper, to_date, to_timestamp, year, month,
    regexp_replace, when, sum as spark_sum, avg, coalesce
)

# ==========================================================
# Spark Session
# ==========================================================

spark = (
    SparkSession.builder
    .appName("RawToTrustedAccidentesMedellin")
    .getOrCreate()
)

# Evita que Spark 3.x aborte cuando encuentra formatos de fecha mixtos.
# Los formatos inválidos quedan como null y luego se filtran.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

BUCKET = "s3://accidentes-medellin-proyecto"

# ==========================================================
# 1. Definición explícita de esquemas
# ==========================================================

# CSV principal: incidentes_viales.csv
# Se define como StringType inicialmente para evitar errores por formatos mixtos,
# y luego se hacen los casteos controlados.
accidentes_schema = StructType([
    StructField("AÃ‘O", StringType(), True),
    StructField("CBML", StringType(), True),
    StructField("CLASE_ACCIDENTE", StringType(), True),
    StructField("DIRECCION", StringType(), True),
    StructField("DIRECCION ENCASILLADA", StringType(), True),
    StructField("DISEÃ‘O", StringType(), True),
    StructField("EXPEDIENTE", StringType(), True),
    StructField("FECHA_ACCIDENTE", StringType(), True),
    StructField("FECHA_ACCIDENTES", StringType(), True),
    StructField("GRAVEDAD_ACCIDENTE", StringType(), True),
    StructField("MES", StringType(), True),
    StructField("NRO_RADICADO", StringType(), True),
    StructField("NUMCOMUNA", StringType(), True),
    StructField("BARRIO", StringType(), True),
    StructField("COMUNA", StringType(), True),
    StructField("LOCATION", StringType(), True),
    StructField("X", StringType(), True),
    StructField("Y", StringType(), True)
])

# CSV clima Open-Meteo generado por nosotros:
# date,rain,precipitation,is_day,year
# Se usa year_csv para no chocar con particiones o funciones year().
clima_schema = StructType([
    StructField("date", StringType(), True),
    StructField("rain", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("is_day", IntegerType(), True),
    StructField("year_csv", IntegerType(), True)
])

# CSV catálogo comunas:
# id_comuna,nombre_comuna,zona
comunas_schema = StructType([
    StructField("id_comuna", IntegerType(), True),
    StructField("nombre_comuna", StringType(), True),
    StructField("zona", StringType(), True)
])

# CSV catálogo tipo_accidente:
# id_tipo,clase_accidente_original,clase_accidente_normalizada,categoria_general,nivel_riesgo,es_valido
tipo_accidente_schema = StructType([
    StructField("id_tipo", IntegerType(), True),
    StructField("clase_accidente_original", StringType(), True),
    StructField("clase_accidente_normalizada", StringType(), True),
    StructField("categoria_general", StringType(), True),
    StructField("nivel_riesgo", StringType(), True),
    StructField("es_valido", IntegerType(), True)
])

# CSV catálogo gravedad_accidente:
# id_gravedad,gravedad_original,gravedad_normalizada,severidad_numerica,descripcion,es_valido
gravedad_schema = StructType([
    StructField("id_gravedad", IntegerType(), True),
    StructField("gravedad_original", StringType(), True),
    StructField("gravedad_normalizada", StringType(), True),
    StructField("severidad_numerica", IntegerType(), True),
    StructField("descripcion", StringType(), True),
    StructField("es_valido", IntegerType(), True)
])

# CSV catálogo clasificacion_lluvia:
# id_clasificacion,lluvia_min,lluvia_max,categoria_lluvia
clasificacion_lluvia_schema = StructType([
    StructField("id_clasificacion", IntegerType(), True),
    StructField("lluvia_min", DoubleType(), True),
    StructField("lluvia_max", DoubleType(), True),
    StructField("categoria_lluvia", StringType(), True)
])

# CSV catálogo franjas_horarias:
# id_franja,hora_inicio,hora_fin,franja
franjas_horarias_schema = StructType([
    StructField("id_franja", IntegerType(), True),
    StructField("hora_inicio", IntegerType(), True),
    StructField("hora_fin", IntegerType(), True),
    StructField("franja", StringType(), True)
])

# ==========================================================
# 2. Lectura CSV accidentes desde raw
# ==========================================================

accidentes_path = f"{BUCKET}/raw/accidentes/"

acc = (
    spark.read
    .option("header", True)
    .option("recursiveFileLookup", True)
    .option("mode", "PERMISSIVE")
    .option("quote", '"')
    .option("escape", '"')
    .schema(accidentes_schema)
    .csv(accidentes_path)
)

acc = (
    acc
    .withColumnRenamed("AÃ‘O", "anio")
    .withColumnRenamed("CBML", "cbml")
    .withColumnRenamed("CLASE_ACCIDENTE", "clase_accidente")
    .withColumnRenamed("DIRECCION", "direccion")
    .withColumnRenamed("DIRECCION ENCASILLADA", "direccion_encasillada")
    .withColumnRenamed("DISEÃ‘O", "diseno")
    .withColumnRenamed("EXPEDIENTE", "expediente")
    .withColumnRenamed("FECHA_ACCIDENTE", "fecha_accidente_raw")
    .withColumnRenamed("FECHA_ACCIDENTES", "fecha_accidentes_raw")
    .withColumnRenamed("GRAVEDAD_ACCIDENTE", "gravedad_accidente")
    .withColumnRenamed("MES", "mes")
    .withColumnRenamed("NRO_RADICADO", "nro_radicado")
    .withColumnRenamed("NUMCOMUNA", "numcomuna")
    .withColumnRenamed("BARRIO", "barrio")
    .withColumnRenamed("COMUNA", "comuna")
    .withColumnRenamed("LOCATION", "location")
    .withColumnRenamed("X", "longitud")
    .withColumnRenamed("Y", "latitud")
)

# Limpieza y normalización de accidentes.
# La fecha se convierte primero a timestamp porque el CSV trae valores como:
# 21/10/2015 05:58:00
# Luego se extrae solamente la fecha para cruzarla con clima diario.
acc = (
    acc
    .filter(col("clase_accidente").isNotNull())
    .filter(col("gravedad_accidente").isNotNull())
    .filter(upper(trim(col("clase_accidente"))) != "CLASE_ACCIDENTE")
    .filter(upper(trim(col("gravedad_accidente"))) != "GRAVEDAD_ACCIDENTE")
    .withColumn(
        "fecha_accidente_ts",
        coalesce(
            to_timestamp(col("fecha_accidente_raw"), "dd/MM/yyyy HH:mm:ss"),
            to_timestamp(col("fecha_accidente_raw"), "d/M/yyyy HH:mm:ss"),
            to_timestamp(col("fecha_accidente_raw"), "dd/MM/yyyy H:mm:ss"),
            to_timestamp(col("fecha_accidente_raw"), "d/M/yyyy H:mm:ss"),
            to_timestamp(col("fecha_accidente_raw"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("fecha_accidente_raw"), "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(col("fecha_accidente_raw"), "dd/MM/yyyy"),
            to_timestamp(col("fecha_accidente_raw"), "d/M/yyyy"),
            to_timestamp(col("fecha_accidente_raw"), "yyyy-MM-dd")
        )
    )
    .withColumn("fecha_accidente", to_date(col("fecha_accidente_ts")))
    .withColumn("anio_accidente", year(col("fecha_accidente")))
    .withColumn("mes_accidente", month(col("fecha_accidente")))
    .withColumn("clase_accidente_original_acc", trim(col("clase_accidente")))
    .withColumn("gravedad_original_acc", trim(col("gravedad_accidente")))
    .withColumn("comuna", upper(trim(col("comuna"))))
    .withColumn("barrio", upper(trim(col("barrio"))))
    .withColumn("numcomuna", col("numcomuna").cast("int"))
    .withColumn("latitud", col("latitud").cast("double"))
    .withColumn("longitud", col("longitud").cast("double"))
)

# Normalización clase accidente
acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    upper(trim(col("clase_accidente_original_acc")))
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "Á", "A")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "É", "E")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "Í", "I")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "Ó", "O")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "Ú", "U")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "Ü", "U")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    regexp_replace(col("clase_accidente_normalizada_acc"), "Ñ", "N")
)

acc = acc.withColumn(
    "clase_accidente_normalizada_acc",
    when(
        col("clase_accidente_normalizada_acc").isin(
            "CAIDA OCUPANTE",
            "CAIDA DE OCUPANTE"
        ),
        "CAIDA OCUPANTE"
    ).otherwise(col("clase_accidente_normalizada_acc"))
)

# Normalización gravedad
acc = acc.withColumn(
    "gravedad_normalizada_acc",
    upper(trim(col("gravedad_original_acc")))
)

acc = acc.withColumn(
    "gravedad_normalizada_acc",
    regexp_replace(col("gravedad_normalizada_acc"), "Ñ", "N")
)

acc = acc.withColumn(
    "gravedad_normalizada_acc",
    regexp_replace(col("gravedad_normalizada_acc"), "DA\\\\XF1OS", "DANOS")
)

acc = acc.withColumn(
    "gravedad_normalizada_acc",
    regexp_replace(col("gravedad_normalizada_acc"), "DAÑOS", "DANOS")
)

acc = acc.withColumn(
    "gravedad_normalizada_acc",
    regexp_replace(col("gravedad_normalizada_acc"), "DAÃ‘OS", "DANOS")
)

# Eliminar registros sin fecha válida, porque no se pueden particionar ni cruzar con clima
acc = acc.filter(col("fecha_accidente").isNotNull())

# Eliminar duplicados por radicado
acc = acc.dropDuplicates(["nro_radicado"])

# ==========================================================
# 3. Lectura clima Open-Meteo desde raw
# ==========================================================

clima_path = f"{BUCKET}/raw/climaopenmeteo/"

clima = (
    spark.read
    .option("header", True)
    .option("recursiveFileLookup", True)
    .option("mode", "PERMISSIVE")
    .schema(clima_schema)
    .csv(clima_path)
)

clima = (
    clima
    .withColumn(
        "fecha_hora",
        coalesce(
            to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm"),
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("date"), "yyyy-MM-dd HH:mm")
        )
    )
    .withColumn("fecha", to_date(col("fecha_hora")))
    .withColumn("rain", col("rain").cast("double"))
    .withColumn("precipitation", col("precipitation").cast("double"))
    .withColumn("is_day", col("is_day").cast("int"))
    .filter(col("fecha").isNotNull())
    .dropDuplicates(["fecha_hora"])
)

clima_diario = (
    clima
    .groupBy("fecha")
    .agg(
        spark_sum("rain").alias("lluvia_total_dia"),
        spark_sum("precipitation").alias("precipitacion_total_dia"),
        avg("rain").alias("lluvia_promedio_hora"),
        spark_sum(when(col("rain") > 0, 1).otherwise(0)).alias("horas_con_lluvia"),
        avg("is_day").alias("promedio_is_day")
    )
)

# ==========================================================
# 4. Lectura catálogos MariaDB desde raw
# ==========================================================

catalogos_path = f"{BUCKET}/raw/catalogo_mariaDB/"

comunas = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(comunas_schema)
    .csv(f"{catalogos_path}/fecha_carga=*/comunas.csv")
    .filter(col("id_comuna").isNotNull())
    .select(
        col("id_comuna").cast("int").alias("cat_id_comuna"),
        upper(trim(col("nombre_comuna"))).alias("cat_nombre_comuna"),
        upper(trim(col("zona"))).alias("cat_zona")
    )
    .dropDuplicates(["cat_id_comuna"])
)

tipo_accidente = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(tipo_accidente_schema)
    .csv(f"{catalogos_path}/fecha_carga=*/tipo_accidente.csv")
    .filter(col("clase_accidente_normalizada").isNotNull())
    .filter(col("es_valido") == 1)
    .select(
        upper(trim(col("clase_accidente_normalizada"))).alias("cat_clase_accidente_normalizada"),
        upper(trim(col("categoria_general"))).alias("categoria_general"),
        upper(trim(col("nivel_riesgo"))).alias("nivel_riesgo"),
        col("es_valido").alias("tipo_accidente_valido")
    )
    .dropDuplicates(["cat_clase_accidente_normalizada"])
)

gravedad = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(gravedad_schema)
    .csv(f"{catalogos_path}/fecha_carga=*/gravedad_accidente.csv")
    .filter(col("gravedad_normalizada").isNotNull())
    .filter(col("es_valido") == 1)
    .select(
        upper(trim(col("gravedad_normalizada"))).alias("cat_gravedad_normalizada"),
        col("severidad_numerica").cast("int").alias("severidad_numerica"),
        col("descripcion").alias("descripcion_gravedad"),
        col("es_valido").alias("gravedad_valida")
    )
    .dropDuplicates(["cat_gravedad_normalizada"])
)

clasificacion_lluvia = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(clasificacion_lluvia_schema)
    .csv(f"{catalogos_path}/fecha_carga=*/clasificacion_lluvia.csv")
    .filter(col("categoria_lluvia").isNotNull())
    .select(
        col("lluvia_min").cast("double").alias("cat_lluvia_min"),
        col("lluvia_max").cast("double").alias("cat_lluvia_max"),
        upper(trim(col("categoria_lluvia"))).alias("categoria_lluvia")
    )
    .dropDuplicates(["cat_lluvia_min", "cat_lluvia_max", "categoria_lluvia"])
)

franjas_horarias = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(franjas_horarias_schema)
    .csv(f"{catalogos_path}/fecha_carga=*/franjas_horarias.csv")
    .filter(col("id_franja").isNotNull())
    .select(
        col("id_franja"),
        col("hora_inicio"),
        col("hora_fin"),
        upper(trim(col("franja"))).alias("franja")
    )
    .dropDuplicates(["id_franja"])
)

# ==========================================================
# 5. Joins de integración
# ==========================================================

df = acc.join(
    clima_diario,
    acc["fecha_accidente"] == clima_diario["fecha"],
    "left"
)

df = df.join(
    comunas,
    df["numcomuna"] == comunas["cat_id_comuna"],
    "left"
)

df = df.join(
    tipo_accidente,
    df["clase_accidente_normalizada_acc"] == tipo_accidente["cat_clase_accidente_normalizada"],
    "left"
)

df = df.join(
    gravedad,
    df["gravedad_normalizada_acc"] == gravedad["cat_gravedad_normalizada"],
    "left"
)

df = df.join(
    clasificacion_lluvia,
    (df["lluvia_total_dia"] >= clasificacion_lluvia["cat_lluvia_min"]) &
    (df["lluvia_total_dia"] <= clasificacion_lluvia["cat_lluvia_max"]),
    "left"
)

# ==========================================================
# 6. Dataset final accidentes_enriquecidos
# ==========================================================

final_df = df.select(
    col("nro_radicado"),
    col("expediente"),
    col("fecha_accidente"),
    col("fecha_accidente_ts"),
    col("anio_accidente"),
    col("mes_accidente"),

    col("clase_accidente_original_acc").alias("clase_accidente_original"),
    col("clase_accidente_normalizada_acc").alias("clase_accidente_normalizada"),
    col("categoria_general"),
    col("nivel_riesgo"),

    col("gravedad_original_acc").alias("gravedad_original"),
    col("gravedad_normalizada_acc").alias("gravedad_normalizada"),
    col("severidad_numerica"),
    col("descripcion_gravedad"),

    col("numcomuna"),
    col("cat_nombre_comuna").alias("nombre_comuna"),
    col("cat_zona").alias("zona"),
    col("barrio"),
    col("direccion"),
    col("latitud"),
    col("longitud"),

    col("lluvia_total_dia"),
    col("precipitacion_total_dia"),
    col("lluvia_promedio_hora"),
    col("horas_con_lluvia"),
    col("categoria_lluvia")
)

# ==========================================================
# 7. Escritura trusted en Parquet
# ==========================================================

final_df.write.mode("overwrite").partitionBy(
    "anio_accidente", "mes_accidente"
).parquet(
    f"{BUCKET}/trusted/accidentes_enriquecidos/"
)

clima_diario.write.mode("overwrite").parquet(
    f"{BUCKET}/trusted/clima_diario/"
)

acc.write.mode("overwrite").partitionBy(
    "anio_accidente", "mes_accidente"
).parquet(
    f"{BUCKET}/trusted/accidentes_limpios/"
)

comunas.write.mode("overwrite").parquet(
    f"{BUCKET}/trusted/catalogos/comunas/"
)

tipo_accidente.write.mode("overwrite").parquet(
    f"{BUCKET}/trusted/catalogos/tipo_accidente/"
)

gravedad.write.mode("overwrite").parquet(
    f"{BUCKET}/trusted/catalogos/gravedad_accidente/"
)

clasificacion_lluvia.write.mode("overwrite").parquet(
    f"{BUCKET}/trusted/catalogos/clasificacion_lluvia/"
)

franjas_horarias.write.mode("overwrite").parquet(
    f"{BUCKET}/trusted/catalogos/franjas_horarias/"
)

# ==========================================================
# 8. Métricas básicas de control
# ==========================================================

print("Proceso raw -> trusted finalizado correctamente.")
print(f"Total registros accidentes_enriquecidos: {final_df.count()}")
print(f"Total registros accidentes_limpios: {acc.count()}")
print(f"Total registros clima_diario: {clima_diario.count()}")

spark.stop()
