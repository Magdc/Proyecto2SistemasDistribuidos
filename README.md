# Proyecto Big Data: Accidentalidad vial en Medellín y clima

## 1. Descripción general

Este proyecto construye un pipeline Big Data en AWS para analizar la accidentalidad vial en Medellín y su relación con condiciones climáticas. Se integran tres fuentes de datos:

1. Archivo CSV de incidentes viales de Medellín.
2. API histórica de clima Open-Meteo.
3. Catálogos almacenados en MariaDB sobre Amazon RDS.

El flujo general corresponde principalmente a un enfoque **ELT**:

```text
Extract  ->  Load en S3 raw  ->  Transform con Spark/EMR  ->  Trusted/Analytics
```

Primero se extraen los datos y se cargan en Amazon S3 en la zona `raw`. Luego se transforman con Apache Spark en Amazon EMR para construir una capa `trusted` en formato Parquet. Finalmente, los datos son catalogados con AWS Glue Data Catalog y consultados con Athena, SparkSQL, Hive y PySpark.

---

## 2. Objetivo del proyecto

Implementar una arquitectura Big Data en AWS que permita:

- Ingestar datos desde archivo, API y base de datos relacional.
- Almacenar datos crudos en un datalake sobre Amazon S3.
- Procesar y enriquecer datos con Apache Spark en Amazon EMR.
- Construir una capa confiable `trusted` en formato Parquet.
- Catalogar los datos con AWS Glue Data Catalog.
- Consultar la información con Athena, Hive, SparkSQL y PySpark.
- Generar análisis descriptivo.
- Construir una aplicación de visualización con Streamlit.
- Exponer la aplicación mediante API Gateway.

---

## 3. Preguntas de negocio

El proyecto permite responder preguntas como:

1. ¿Cuál fue el año con mayor cantidad de accidentes?
2. ¿Qué comuna o corregimiento concentra más accidentes?
3. ¿Qué comuna tiene más accidentes con heridos?
4. ¿Cuántos accidentes ocurren en días con lluvia fuerte?
5. ¿Qué tipos de accidente son más frecuentes cuando hay lluvia fuerte?
6. ¿Cómo cambia la gravedad de los accidentes según la categoría de lluvia?
7. ¿Qué barrios concentran más accidentes?
8. ¿Qué zonas de Medellín tienen mayor accidentalidad?
9. ¿Cuáles fueron los días con más accidentes?
10. ¿Qué problemas de calidad se detectan en la tabla maestra?

---

## 4. Arquitectura general

```text
CSV incidentes viales       API Open-Meteo        RDS MariaDB
        │                         │                    │
        └──────────────┬──────────┴──────────────┬─────┘
                       │                         │
                       ▼                         ▼
                    Amazon EC2 - scripts de ingesta
                       │
                       ▼
               Amazon S3 - zona raw
                       │
                       ▼
          Amazon EMR + Apache Spark
             Transformación raw -> trusted
                       │
                       ▼
               Amazon S3 - zona trusted
                       │
                       ▼
              AWS Glue Data Catalog
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
     Athena        PySpark       Streamlit
```

---

## 5. Servicios AWS utilizados

| Servicio | Uso en el proyecto |
|---|---|
| Amazon S3 | Datalake para zonas raw, trusted y analytics |
| Amazon EC2 | Máquina de apoyo para ingesta, scripts y Streamlit |
| Amazon RDS MariaDB | Base de datos relacional para catálogos |
| Amazon EMR | Cluster Big Data para ejecutar Spark |
| Apache Spark | Transformación, limpieza, joins y análisis descriptivo |
| AWS Glue Data Catalog | Catálogo de tablas sobre datos en S3 |
| AWS Glue Crawler | Descubrimiento de esquemas en S3 |
| Amazon Athena | Consultas SQL sobre el datalake |
| Streamlit | Aplicación de visualización |
| API Gateway | Exposición HTTP de la aplicación |
| IAM Roles del laboratorio | Permisos para S3, EMR, Glue y EC2 |

---

## 6. Estructura del bucket S3

Bucket utilizado:

```text
s3://accidentes-medellin-proyecto/
```

Estructura recomendada e idempotente:

```text
accidentes-medellin-proyecto/
│
├── raw/
│   ├── accidentes/
│   │   └── incidentes_viales.csv
│   │
│   ├── climaopenmeteo/
│   │   ├── year=2015/clima_openmeteo_2015.csv
│   │   ├── year=2016/clima_openmeteo_2016.csv
│   │   ├── year=2017/clima_openmeteo_2017.csv
│   │   ├── year=2018/clima_openmeteo_2018.csv
│   │   ├── year=2019/clima_openmeteo_2019.csv
│   │   └── year=2020/clima_openmeteo_2020.csv
│   │
│   └── catalogo_mariaDB/
│       ├── comunas.csv
│       ├── tipo_accidente.csv
│       ├── gravedad_accidente.csv
│       ├── clasificacion_lluvia.csv
│       └── franjas_horarias.csv
│
├── scripts/
│   ├── raw_to_trusted.py
│   └── analisis_descriptivo_pyspark.py
│
├── trusted/
│   ├── accidentes_enriquecidos/
│   ├── accidentes_limpios/
│   ├── clima_diario/
│   └── catalogos/
│       ├── comunas/
│       ├── tipo_accidente/
│       ├── gravedad_accidente/
│       ├── clasificacion_lluvia/
│       └── franjas_horarias/
│
├── analytics/
│   ├── anio_mas_accidentes/
│   ├── comunas_mas_accidentes/
│   ├── comunas_mas_accidentes_heridos/
│   ├── accidentes_por_lluvia/
│   ├── tipos_accidente_lluvia_fuerte/
│   ├── accidentes_por_gravedad/
│   ├── lluvia_por_gravedad/
│   ├── dias_mas_accidentes/
│   └── resumen_calidad/
│
└── emr-logs/
```

---

## 7. Estructura del proyecto en EC2

```text
/home/ubuntu/proyecto2/
│
├── data/
│   └── incidentes_viales.csv
│
├── scripts/
│   ├── upload_incidentes_to_s3.sh
│   ├── ingest_openmeteo_to_s3.py
│   ├── export_mariadb_catalogos_to_s3.sh
│   └── mariadb_config.env
│
├── spark/
│   ├── raw_to_trusted.py
│   └── analisis_descriptivo_pyspark.py
│
├── clima/
│   ├── clima_openmeteo_2015.csv
│   ├── clima_openmeteo_2016.csv
│   ├── clima_openmeteo_2017.csv
│   ├── clima_openmeteo_2018.csv
│   ├── clima_openmeteo_2019.csv
│   └── clima_openmeteo_2020.csv
│
├── catalogos_mariadb/
│   ├── comunas.csv
│   ├── tipo_accidente.csv
│   ├── gravedad_accidente.csv
│   ├── clasificacion_lluvia.csv
│   └── franjas_horarias.csv
│
├── streamlit/
│   └── app.py
│
├── logs/
│   ├── upload_incidentes.log
│   ├── ingest_openmeteo.log
│   └── export_mariadb_catalogos.log
│
└── venv/
```

---

## 8. Configuración inicial en EC2

Instalar dependencias:

```bash
sudo apt update
sudo apt install -y python3-venv python3-pip mysql-client

mkdir -p ~/proyecto2/{data,scripts,spark,clima,catalogos_mariadb,streamlit,logs}
cd ~/proyecto2
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install pandas requests boto3 streamlit matplotlib pyarrow s3fs
```

El rol de la EC2 debe tener permisos para leer y escribir en S3. En el laboratorio se usaron roles existentes como `LabRole` o el perfil de instancia disponible en AWS Academy.

---

## 9. Configuración de RDS MariaDB

Base de datos utilizada:

```sql
CREATE DATABASE IF NOT EXISTS accidentalidad_medellin
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;
```

Tablas catálogo:

```text
comunas
tipo_accidente
gravedad_accidente
clasificacion_lluvia
franjas_horarias
```

### 9.1 Comunas y corregimientos

La tabla `comunas` contiene las 16 comunas urbanas y los 5 corregimientos de Medellín. Esto fue necesario porque en el CSV original aparecen códigos como `50`, `60`, `70`, `80` y `90`, que corresponden a corregimientos.

```sql
INSERT INTO comunas (id_comuna, nombre_comuna, zona)
VALUES
(50, 'SAN SEBASTIAN DE PALMITAS', 'CORREGIMIENTO'),
(60, 'SAN CRISTOBAL', 'CORREGIMIENTO'),
(70, 'ALTAVISTA', 'CORREGIMIENTO'),
(80, 'SAN ANTONIO DE PRADO', 'CORREGIMIENTO'),
(90, 'SANTA ELENA', 'CORREGIMIENTO')
ON DUPLICATE KEY UPDATE
    nombre_comuna = VALUES(nombre_comuna),
    zona = VALUES(zona);
```

### 9.2 Clasificación de lluvia diaria

La lluvia se clasifica usando el acumulado diario en milímetros:

```sql
TRUNCATE TABLE clasificacion_lluvia;

INSERT INTO clasificacion_lluvia
(lluvia_min, lluvia_max, categoria_lluvia)
VALUES
(0.00, 0.00, 'SIN LLUVIA'),
(0.01, 10.00, 'LLUVIA LIGERA'),
(10.01, 30.00, 'LLUVIA MODERADA'),
(30.01, 70.00, 'LLUVIA FUERTE'),
(70.01, 999.00, 'LLUVIA MUY FUERTE');
```

---

## 10. Scripts de ingesta

### 10.1 `upload_incidentes_to_s3.sh`

Carga el CSV de incidentes a una ruta fija:

```text
s3://accidentes-medellin-proyecto/raw/accidentes/incidentes_viales.csv
```

Ejecución:

```bash
~/proyecto2/scripts/upload_incidentes_to_s3.sh
```

### 10.2 `ingest_openmeteo_to_s3.py`

Consulta Open-Meteo para Medellín y escribe un archivo por año:

```text
raw/climaopenmeteo/year=2015/clima_openmeteo_2015.csv
...
raw/climaopenmeteo/year=2020/clima_openmeteo_2020.csv
```

Ejecución:

```bash
~/proyecto2/venv/bin/python ~/proyecto2/scripts/ingest_openmeteo_to_s3.py
```

### 10.3 `export_mariadb_catalogos_to_s3.sh`

Exporta las tablas catálogo desde MariaDB hacia S3:

```text
raw/catalogo_mariaDB/comunas.csv
raw/catalogo_mariaDB/tipo_accidente.csv
raw/catalogo_mariaDB/gravedad_accidente.csv
raw/catalogo_mariaDB/clasificacion_lluvia.csv
raw/catalogo_mariaDB/franjas_horarias.csv
```

Ejecución:

```bash
~/proyecto2/scripts/export_mariadb_catalogos_to_s3.sh
```

---

## 11. Configuración de Amazon EMR

Aplicaciones seleccionadas:

```text
Hadoop
Hive
Spark
JupyterEnterpriseGateway
JupyterHub
Hue opcional
Zeppelin opcional
```

Opciones de Glue Data Catalog activadas:

```text
Use for Hive table metadata
Use for Spark table metadata
```

Configuración usada en laboratorio:

| Parámetro | Valor recomendado |
|---|---|
| Subred | Pública, por facilidad de conexión SSH |
| Logs | `s3://accidentes-medellin-proyecto/emr-logs/` |
| Bootstrap actions | Ninguna |
| Service role | Rol existente del laboratorio |
| EC2 instance profile | Rol/perfil existente del laboratorio |
| Key pair | Llave `.pem` usada para SSH |

Conexión al nodo master:

```bash
ssh -i /ruta/llave.pem hadoop@DNS_PUBLICO_MASTER_EMR
```

---

## 12. Transformación raw -> trusted

Script principal:

```text
spark/raw_to_trusted.py
```

Ubicación en S3:

```text
s3://accidentes-medellin-proyecto/scripts/raw_to_trusted.py
```

Subida del script:

```bash
aws s3 cp ~/proyecto2/spark/raw_to_trusted.py \
  s3://accidentes-medellin-proyecto/scripts/raw_to_trusted.py
```

Ejecución en EMR:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://accidentes-medellin-proyecto/scripts/raw_to_trusted.py
```

### 12.1 Transformaciones realizadas

El script realiza:

- Lectura de archivos CSV con esquemas explícitos.
- Parseo de fechas con hora, por ejemplo `21/10/2015 05:58:00`.
- Limpieza de encabezados repetidos.
- Normalización de `NUMCOMUNA`.
- Conversión de valores como `01`, `02`, `04` a enteros válidos.
- Manejo de valores no informados como `Sin Inf`, `SN`, `AU`, `In`, `0`, `00`.
- Normalización de tipos de accidente.
- Normalización de gravedad.
- Deduplicación por `nro_radicado`.
- Deduplicación de clima por `fecha_hora`.
- Agregación diaria del clima.
- Join con catálogos MariaDB.
- Escritura en Parquet.
- Particionamiento por `anio_accidente` y `mes_accidente`.

### 12.2 Salidas generadas

```text
trusted/accidentes_enriquecidos/
trusted/accidentes_limpios/
trusted/clima_diario/
trusted/catalogos/comunas/
trusted/catalogos/tipo_accidente/
trusted/catalogos/gravedad_accidente/
trusted/catalogos/clasificacion_lluvia/
trusted/catalogos/franjas_horarias/
```

---

## 13. Idempotencia e incrementalidad

El proyecto fue ajustado para evitar duplicados al ejecutar varias veces el pipeline.

### 13.1 Reglas aplicadas

| Fuente | Regla de idempotencia |
|---|---|
| Accidentes CSV | Ruta fija `raw/accidentes/incidentes_viales.csv` |
| Clima Open-Meteo | Ruta fija por año `raw/climaopenmeteo/year=YYYY/` |
| Catálogos MariaDB | Ruta fija por tabla en `raw/catalogo_mariaDB/` |
| Trusted | Reconstrucción con `overwrite` |
| Analytics | Reconstrucción con `overwrite` |

### 13.2 Problemas corregidos

- Se eliminaron carpetas basadas en `fecha_carga` para evitar leer la misma fuente varias veces.
- Se aplicó `dropDuplicates(["fecha_hora"])` en clima para evitar más de 24 horas de lluvia por día.
- Se aplicó `dropDuplicates(["nro_radicado"])` en accidentes.
- Se reconstruye `trusted` de manera determinística.

---

## 14. AWS Glue Data Catalog

Base de datos:

```text
db_accidentes_medellin
```

Location:

```text
s3://accidentes-medellin-proyecto/trusted/
```

Crawler:

```text
crawler_trusted_accidentes_medellin
```

Ruta del crawler:

```text
s3://accidentes-medellin-proyecto/trusted/
```

Frecuencia:

```text
On demand
```

Rol:

```text
LabRole o rol equivalente del laboratorio
```

Tablas esperadas:

```text
accidentes_enriquecidos
accidentes_limpios
clima_diario
comunas
tipo_accidente
gravedad_accidente
clasificacion_lluvia
franjas_horarias
```

---

## 15. Consultas de negocio en Athena

Base:

```sql
db_accidentes_medellin
```

Tabla principal:

```sql
accidentes_enriquecidos
```

### 15.1 Año con más accidentes

```sql
SELECT 
    anio_accidente,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
GROUP BY anio_accidente
ORDER BY total_accidentes DESC;
```

### 15.2 Comuna o corregimiento con más accidentes

```sql
SELECT 
    numcomuna,
    COALESCE(nombre_comuna, 'SIN CATALOGO') AS nombre_comuna,
    COALESCE(zona, 'SIN ZONA') AS zona,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
GROUP BY 
    numcomuna,
    COALESCE(nombre_comuna, 'SIN CATALOGO'),
    COALESCE(zona, 'SIN ZONA')
ORDER BY total_accidentes DESC
LIMIT 20;
```

### 15.3 Comuna con más accidentes con heridos

```sql
SELECT 
    numcomuna,
    COALESCE(nombre_comuna, 'SIN CATALOGO') AS nombre_comuna,
    COALESCE(zona, 'SIN ZONA') AS zona,
    COUNT(*) AS total_accidentes_con_heridos
FROM db_accidentes_medellin.accidentes_enriquecidos
WHERE gravedad_normalizada = 'CON HERIDOS'
GROUP BY 
    numcomuna,
    COALESCE(nombre_comuna, 'SIN CATALOGO'),
    COALESCE(zona, 'SIN ZONA')
ORDER BY total_accidentes_con_heridos DESC
LIMIT 20;
```

### 15.4 Accidentes por categoría de lluvia

```sql
SELECT 
    COALESCE(categoria_lluvia, 'SIN DATO CLIMA') AS categoria_lluvia,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
GROUP BY COALESCE(categoria_lluvia, 'SIN DATO CLIMA')
ORDER BY total_accidentes DESC;
```

### 15.5 Tipos de accidente más comunes con lluvia fuerte

```sql
SELECT 
    clase_accidente_normalizada,
    COALESCE(categoria_general, 'SIN CATEGORIA') AS categoria_general,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
WHERE categoria_lluvia = 'LLUVIA FUERTE'
GROUP BY 
    clase_accidente_normalizada,
    COALESCE(categoria_general, 'SIN CATEGORIA')
ORDER BY total_accidentes DESC
LIMIT 15;
```

---

## 16. Validaciones de calidad en Athena

### 16.1 Resumen general de calidad

```sql
SELECT
    COUNT(*) AS total_registros,

    SUM(CASE WHEN fecha_accidente IS NULL THEN 1 ELSE 0 END) AS fechas_nulas,
    SUM(CASE WHEN anio_accidente IS NULL THEN 1 ELSE 0 END) AS anios_nulos,
    SUM(CASE WHEN mes_accidente IS NULL THEN 1 ELSE 0 END) AS meses_nulos,

    SUM(CASE WHEN nombre_comuna IS NULL THEN 1 ELSE 0 END) AS comunas_sin_catalogo,
    SUM(CASE WHEN categoria_general IS NULL THEN 1 ELSE 0 END) AS tipos_accidente_sin_catalogo,
    SUM(CASE WHEN severidad_numerica IS NULL THEN 1 ELSE 0 END) AS gravedades_sin_catalogo,

    SUM(CASE WHEN lluvia_total_dia IS NULL THEN 1 ELSE 0 END) AS registros_sin_clima,
    SUM(CASE WHEN horas_con_lluvia > 24 THEN 1 ELSE 0 END) AS registros_con_horas_lluvia_invalidas
FROM db_accidentes_medellin.accidentes_enriquecidos;
```

### 16.2 Horas de lluvia imposibles

```sql
SELECT 
    fecha_accidente,
    lluvia_total_dia,
    horas_con_lluvia,
    categoria_lluvia,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
WHERE horas_con_lluvia > 24
GROUP BY 
    fecha_accidente,
    lluvia_total_dia,
    horas_con_lluvia,
    categoria_lluvia
ORDER BY horas_con_lluvia DESC;
```

Resultado esperado después de la corrección:

```text
0 filas
```

### 16.3 Duplicados por radicado

```sql
SELECT 
    nro_radicado,
    COUNT(*) AS veces
FROM db_accidentes_medellin.accidentes_enriquecidos
WHERE nro_radicado IS NOT NULL
  AND TRIM(nro_radicado) <> ''
GROUP BY nro_radicado
HAVING COUNT(*) > 1
ORDER BY veces DESC
LIMIT 50;
```

### 16.4 Comunas sin catálogo

```sql
SELECT 
    numcomuna,
    COUNT(*) AS total_registros
FROM db_accidentes_medellin.accidentes_enriquecidos
WHERE nombre_comuna IS NULL
GROUP BY numcomuna
ORDER BY total_registros DESC;
```

### 16.5 Registros sin clima

```sql
SELECT 
    anio_accidente,
    COUNT(*) AS accidentes_sin_clima
FROM db_accidentes_medellin.accidentes_enriquecidos
WHERE lluvia_total_dia IS NULL
   OR horas_con_lluvia IS NULL
   OR categoria_lluvia IS NULL
GROUP BY anio_accidente
ORDER BY anio_accidente;
```

---

## 17. Análisis descriptivo con PySpark

Script:

```text
spark/analisis_descriptivo_pyspark.py
```

Ubicación en S3:

```text
s3://accidentes-medellin-proyecto/scripts/analisis_descriptivo_pyspark.py
```

Ejecución:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://accidentes-medellin-proyecto/scripts/analisis_descriptivo_pyspark.py
```

El script genera análisis como:

- Año con más accidentes.
- Comunas con más accidentes.
- Comunas con más accidentes con heridos.
- Accidentes por categoría de lluvia.
- Tipos de accidente más comunes con lluvia fuerte.
- Accidentes por gravedad.
- Promedio de lluvia por gravedad.
- Días con más accidentes.
- Resumen de calidad.

Resultados en S3:

```text
analytics/anio_mas_accidentes/
analytics/comunas_mas_accidentes/
analytics/comunas_mas_accidentes_heridos/
analytics/accidentes_por_lluvia/
analytics/tipos_accidente_lluvia_fuerte/
analytics/accidentes_por_gravedad/
analytics/lluvia_por_gravedad/
analytics/dias_mas_accidentes/
analytics/resumen_calidad/
```

---

## 18. Aplicación de visualización con Streamlit

Archivo:

```text
streamlit/app.py
```

La aplicación consume datos desde la capa `analytics` en S3 y muestra:

- Métricas generales.
- Accidentes por año.
- Top comunas y corregimientos.
- Accidentes por categoría de lluvia.
- Accidentes por gravedad.
- Tablas de apoyo.

Ejecución en EC2:

```bash
cd ~/proyecto2/streamlit
source ~/proyecto2/venv/bin/activate

streamlit run app.py \
  --server.port 8501 \
  --server.address 0.0.0.0
```

Acceso directo:

```text
http://IP_PUBLICA_EC2:8501
```

Security Group requerido:

```text
Custom TCP
Puerto: 8501
Origen: IP personal o 0.0.0.0/0 para pruebas académicas
```

---

## 19. Exposición con API Gateway

Arquitectura:

```text
Usuario
   │
   ▼
API Gateway HTTP API
   │
   ▼
HTTP proxy integration
   │
   ▼
EC2 pública:8501
   │
   ▼
Streamlit app
   │
   ▼
S3 analytics / trusted
```

Configuración sugerida:

```text
API type: HTTP API
Integration type: HTTP URL
Backend URL: http://IP_PUBLICA_EC2:8501
Route: ANY /{proxy+}
Stage: prod
```

---

## 20. Orden de ejecución completo

### 20.1 Limpiar raw anterior si se viene de una versión no idempotente

```bash
aws s3 rm s3://accidentes-medellin-proyecto/raw/accidentes/ --recursive
aws s3 rm s3://accidentes-medellin-proyecto/raw/climaopenmeteo/ --recursive
aws s3 rm s3://accidentes-medellin-proyecto/raw/catalogo_mariaDB/ --recursive
```

### 20.2 Cargar fuentes a raw

```bash
~/proyecto2/scripts/upload_incidentes_to_s3.sh
~/proyecto2/venv/bin/python ~/proyecto2/scripts/ingest_openmeteo_to_s3.py
~/proyecto2/scripts/export_mariadb_catalogos_to_s3.sh
```

### 20.3 Subir scripts Spark a S3

```bash
aws s3 cp ~/proyecto2/spark/raw_to_trusted.py \
  s3://accidentes-medellin-proyecto/scripts/raw_to_trusted.py

aws s3 cp ~/proyecto2/spark/analisis_descriptivo_pyspark.py \
  s3://accidentes-medellin-proyecto/scripts/analisis_descriptivo_pyspark.py
```

### 20.4 Ejecutar transformación raw -> trusted

```bash
aws s3 rm s3://accidentes-medellin-proyecto/trusted/ --recursive

spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://accidentes-medellin-proyecto/scripts/raw_to_trusted.py
```

### 20.5 Ejecutar análisis descriptivo PySpark

```bash
aws s3 rm s3://accidentes-medellin-proyecto/analytics/ --recursive

spark-submit \
  --master yarn \
  --deploy-mode client \
  s3://accidentes-medellin-proyecto/scripts/analisis_descriptivo_pyspark.py
```

### 20.6 Ejecutar Glue Crawler

```text
AWS Glue -> Crawlers -> crawler_trusted_accidentes_medellin -> Run crawler
```

### 20.7 Consultar con Athena

```sql
SHOW TABLES IN db_accidentes_medellin;

SELECT *
FROM db_accidentes_medellin.accidentes_enriquecidos
LIMIT 10;
```

### 20.8 Ejecutar Streamlit

```bash
cd ~/proyecto2/streamlit
source ~/proyecto2/venv/bin/activate

streamlit run app.py \
  --server.port 8501 \
  --server.address 0.0.0.0
```

---

## 21. Estructura sugerida del repositorio GitHub

```text
proyecto-bigdata-accidentes-medellin/
│
├── README.md
│
├── scripts/
│   ├── upload_incidentes_to_s3.sh
│   ├── ingest_openmeteo_to_s3.py
│   ├── export_mariadb_catalogos_to_s3.sh
│   └── mariadb_config.env.example
│
├── spark/
│   ├── raw_to_trusted.py
│   └── analisis_descriptivo_pyspark.py
│
├── sql/
│   ├── create_catalogos_mariadb.sql
│   ├── athena_business_queries.sql
│   └── athena_quality_queries.sql
│
├── streamlit/
│   └── app.py
│
├── docs/
│   ├── arquitectura.md
│   ├── evidencias.md
│   └── decisiones_tecnicas.md
│
└── img/
    ├── s3_raw.png
    ├── s3_trusted.png
    ├── glue_crawler.png
    ├── athena_queries.png
    ├── emr_cluster.png
    └── streamlit_dashboard.png
```

---

## 22. Problemas encontrados y soluciones

| Problema | Causa | Solución |
|---|---|---|
| Columnas ambiguas en Spark | Joins con columnas repetidas | Uso de alias para columnas de catálogos |
| Error con fechas | El CSV tenía fecha y hora | Uso de `to_timestamp` con múltiples formatos |
| Horas de lluvia mayores a 24 | Clima duplicado en raw | Rutas fijas y deduplicación por `fecha_hora` |
| Comunas sin catálogo | Faltaban corregimientos y había valores sucios | Se agregaron corregimientos y se normalizó `NUMCOMUNA` |
| Lluvia fuerte desde 7.6 mm | Catálogo inicial pensado con umbral bajo | Se ajustó la clasificación para lluvia diaria |
| Duplicados por cargas repetidas | Uso de `fecha_carga` en raw | Diseño idempotente con rutas determinísticas |
| Restricciones IAM | AWS Academy no permite crear roles | Uso de roles existentes del laboratorio |

---

## 23. Evidencias recomendadas para la entrega

Se recomienda incluir capturas de:

1. Bucket S3 con las carpetas `raw`, `trusted` y `analytics`.
2. Archivos raw de accidentes, clima y catálogos.
3. RDS MariaDB con tablas catálogo.
4. EMR cluster configurado.
5. Ejecución de `spark-submit` raw -> trusted.
6. Archivos Parquet generados en `trusted`.
7. Glue Database `db_accidentes_medellin`.
8. Glue Crawler completado.
9. Tablas creadas en Glue Data Catalog.
10. Consultas de negocio en Athena.
11. Consultas de calidad en Athena.
12. Ejecución de análisis descriptivo PySpark.
13. Carpeta `analytics` generada.
14. Dashboard Streamlit funcionando.
15. API Gateway exponiendo la aplicación.

---

## 24. Conclusión

El proyecto implementa un pipeline Big Data completo sobre AWS para analizar la accidentalidad vial en Medellín. La solución integra datos desde archivos, API y base relacional, los almacena en un datalake sobre S3, los transforma con Spark en EMR y los cataloga con Glue para consultas en Athena y PySpark.

Durante el desarrollo se identificaron y corrigieron problemas reales de calidad de datos, como duplicidad climática, comunas no catalogadas, corregimientos faltantes, formatos de fecha mixtos y valores inconsistentes en `NUMCOMUNA`. Además, se ajustó el diseño para hacerlo idempotente, evitando que múltiples ejecuciones del pipeline dupliquen datos.

Finalmente, los resultados se disponibilizan en una capa `analytics` y se visualizan mediante una aplicación Streamlit, permitiendo explorar de manera clara la relación entre accidentalidad, ubicación geográfica, gravedad y condiciones climáticas.
