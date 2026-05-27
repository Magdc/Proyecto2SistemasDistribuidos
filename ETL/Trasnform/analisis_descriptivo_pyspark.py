from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, desc, avg, round, coalesce, lit, sum as spark_sum
)

spark = (
    SparkSession.builder
    .appName("AnalisisDescriptivoAccidentesMedellin")
    .getOrCreate()
)

BUCKET = "s3://accidentes-medellin-proyecto"

df = spark.read.parquet(
    f"{BUCKET}/trusted/accidentes_enriquecidos/"
)

print("======================================")
print("ESQUEMA TABLA ACCIDENTES ENRIQUECIDOS")
print("======================================")
df.printSchema()

print("======================================")
print("TOTAL DE REGISTROS")
print("======================================")
print(df.count())

# ==========================================================
# 1. Año con más accidentes
# ==========================================================

print("======================================")
print("1. Año con más accidentes")
print("======================================")

anio_mas_accidentes = (
    df.groupBy("anio_accidente")
    .agg(count("*").alias("total_accidentes"))
    .orderBy(desc("total_accidentes"))
)

anio_mas_accidentes.show(20, truncate=False)

# ==========================================================
# 2. Comuna o corregimiento con más accidentes
# ==========================================================

print("======================================")
print("2. Comuna o corregimiento con más accidentes")
print("======================================")

comunas_mas_accidentes = (
    df.groupBy(
        "numcomuna",
        "nombre_comuna",
        "zona"
    )
    .agg(count("*").alias("total_accidentes"))
    .orderBy(desc("total_accidentes"))
)

comunas_mas_accidentes.show(20, truncate=False)

# ==========================================================
# 3. Comuna con más accidentes con heridos
# ==========================================================

print("======================================")
print("3. Comuna con más accidentes con heridos")
print("======================================")

comunas_heridos = (
    df.filter(col("gravedad_normalizada") == "CON HERIDOS")
    .groupBy(
        "numcomuna",
        "nombre_comuna",
        "zona"
    )
    .agg(count("*").alias("total_accidentes_con_heridos"))
    .orderBy(desc("total_accidentes_con_heridos"))
)

comunas_heridos.show(20, truncate=False)

# ==========================================================
# 4. Accidentes por categoría de lluvia
# ==========================================================

print("======================================")
print("4. Accidentes por categoría de lluvia")
print("======================================")

accidentes_por_lluvia = (
    df.withColumn(
        "categoria_lluvia_limpia",
        coalesce(col("categoria_lluvia"), lit("SIN DATO CLIMA"))
    )
    .groupBy("categoria_lluvia_limpia")
    .agg(count("*").alias("total_accidentes"))
    .orderBy(desc("total_accidentes"))
)

accidentes_por_lluvia.show(20, truncate=False)

# ==========================================================
# 5. Tipos de accidente más comunes en lluvia fuerte
# ==========================================================

print("======================================")
print("5. Tipos de accidente más comunes con lluvia fuerte")
print("======================================")

tipos_lluvia_fuerte = (
    df.filter(col("categoria_lluvia") == "LLUVIA FUERTE")
    .groupBy(
        "clase_accidente_normalizada",
        "categoria_general"
    )
    .agg(count("*").alias("total_accidentes"))
    .orderBy(desc("total_accidentes"))
)

tipos_lluvia_fuerte.show(20, truncate=False)

# ==========================================================
# 6. Accidentes por gravedad
# ==========================================================

print("======================================")
print("6. Accidentes por gravedad")
print("======================================")

accidentes_por_gravedad = (
    df.groupBy("gravedad_normalizada")
    .agg(count("*").alias("total_accidentes"))
    .orderBy(desc("total_accidentes"))
)

accidentes_por_gravedad.show(20, truncate=False)

# ==========================================================
# 7. Promedio de lluvia por gravedad
# ==========================================================

print("======================================")
print("7. Promedio de lluvia por gravedad")
print("======================================")

lluvia_por_gravedad = (
    df.groupBy("gravedad_normalizada")
    .agg(
        count("*").alias("total_accidentes"),
        round(avg("lluvia_total_dia"), 2).alias("promedio_lluvia_total_dia"),
        round(avg("horas_con_lluvia"), 2).alias("promedio_horas_con_lluvia")
    )
    .orderBy(desc("promedio_lluvia_total_dia"))
)

lluvia_por_gravedad.show(20, truncate=False)

# ==========================================================
# 8. Días con más accidentes
# ==========================================================

print("======================================")
print("8. Días con más accidentes")
print("======================================")

dias_mas_accidentes = (
    df.groupBy(
        "fecha_accidente",
        "categoria_lluvia",
        "lluvia_total_dia",
        "horas_con_lluvia"
    )
    .agg(count("*").alias("total_accidentes"))
    .orderBy(desc("total_accidentes"))
)

dias_mas_accidentes.show(20, truncate=False)

# ==========================================================
# 9. Resumen de calidad de datos
# ==========================================================

print("======================================")
print("9. Resumen de calidad de datos")
print("======================================")

calidad = df.select(
    count("*").alias("total_registros"),

    spark_sum(
        col("fecha_accidente").isNull().cast("int")
    ).alias("fechas_nulas"),

    spark_sum(
        col("anio_accidente").isNull().cast("int")
    ).alias("anios_nulos"),

    spark_sum(
        col("mes_accidente").isNull().cast("int")
    ).alias("meses_nulos"),

    spark_sum(
        col("nombre_comuna").isNull().cast("int")
    ).alias("comunas_sin_catalogo"),

    spark_sum(
        col("categoria_general").isNull().cast("int")
    ).alias("tipos_accidente_sin_catalogo"),

    spark_sum(
        col("severidad_numerica").isNull().cast("int")
    ).alias("gravedades_sin_catalogo"),

    spark_sum(
        col("lluvia_total_dia").isNull().cast("int")
    ).alias("registros_sin_clima"),

    spark_sum(
        (col("horas_con_lluvia") > 24).cast("int")
    ).alias("registros_con_horas_lluvia_invalidas")
)

calidad.show(truncate=False)

# ==========================================================
# 10. Guardar resultados analíticos en S3
# ==========================================================

anio_mas_accidentes.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/anio_mas_accidentes/"
)

comunas_mas_accidentes.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/comunas_mas_accidentes/"
)

comunas_heridos.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/comunas_mas_accidentes_heridos/"
)

accidentes_por_lluvia.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/accidentes_por_lluvia/"
)

tipos_lluvia_fuerte.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/tipos_accidente_lluvia_fuerte/"
)

accidentes_por_gravedad.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/accidentes_por_gravedad/"
)

lluvia_por_gravedad.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/lluvia_por_gravedad/"
)

dias_mas_accidentes.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/dias_mas_accidentes/"
)

calidad.write.mode("overwrite").parquet(
    f"{BUCKET}/analytics/resumen_calidad/"
)

print("Análisis descriptivo PySpark finalizado correctamente.")

spark.stop()