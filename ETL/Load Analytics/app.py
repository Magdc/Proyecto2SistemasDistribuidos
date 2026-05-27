import streamlit as st
import pandas as pd
import boto3
import io
import matplotlib.pyplot as plt

st.set_page_config(
    page_title="Dashboard Accidentes Medellín",
    layout="wide"
)

BUCKET = "accidentes-medellin-proyecto"

st.title("Dashboard de accidentalidad vial en Medellín")
st.write(
    "Aplicación de visualización construida a partir de la capa trusted y analytics del datalake en S3."
)

@st.cache_data
def read_parquet_from_s3(prefix):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not files:
        return pd.DataFrame()

    dfs = []
    for key in files:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj["Body"].read()
        dfs.append(pd.read_parquet(io.BytesIO(data)))

    return pd.concat(dfs, ignore_index=True)

anio_df = read_parquet_from_s3("analytics/anio_mas_accidentes/")
comunas_df = read_parquet_from_s3("analytics/comunas_mas_accidentes/")
lluvia_df = read_parquet_from_s3("analytics/accidentes_por_lluvia/")
gravedad_df = read_parquet_from_s3("analytics/accidentes_por_gravedad/")
calidad_df = read_parquet_from_s3("analytics/resumen_calidad/")

st.header("Resumen general")

if not calidad_df.empty:
    total_registros = int(calidad_df["total_registros"].iloc[0])
    registros_sin_clima = int(calidad_df["registros_sin_clima"].iloc[0])
    comunas_sin_catalogo = int(calidad_df["comunas_sin_catalogo"].iloc[0])

    col1, col2, col3 = st.columns(3)
    col1.metric("Total accidentes", f"{total_registros:,}")
    col2.metric("Registros sin clima", registros_sin_clima)
    col3.metric("Comunas sin catálogo", comunas_sin_catalogo)

st.header("Accidentes por año")

if not anio_df.empty:
    anio_df = anio_df.sort_values("anio_accidente")

    fig, ax = plt.subplots()
    ax.bar(anio_df["anio_accidente"].astype(str), anio_df["total_accidentes"])
    ax.set_xlabel("Año")
    ax.set_ylabel("Total accidentes")
    ax.set_title("Total de accidentes por año")
    st.pyplot(fig)

    st.dataframe(anio_df)

st.header("Top comunas o corregimientos con más accidentes")

if not comunas_df.empty:
    top_comunas = comunas_df.head(15)

    fig, ax = plt.subplots()
    ax.barh(top_comunas["nombre_comuna"].fillna("SIN CATALOGO"), top_comunas["total_accidentes"])
    ax.set_xlabel("Total accidentes")
    ax.set_ylabel("Comuna o corregimiento")
    ax.set_title("Top comunas o corregimientos con más accidentes")
    ax.invert_yaxis()
    st.pyplot(fig)

    st.dataframe(top_comunas)

st.header("Accidentes por categoría de lluvia")

if not lluvia_df.empty:
    fig, ax = plt.subplots()
    ax.bar(lluvia_df["categoria_lluvia_limpia"], lluvia_df["total_accidentes"])
    ax.set_xlabel("Categoría de lluvia")
    ax.set_ylabel("Total accidentes")
    ax.set_title("Accidentes por categoría de lluvia")
    plt.xticks(rotation=30)
    st.pyplot(fig)

    st.dataframe(lluvia_df)

st.header("Accidentes por gravedad")

if not gravedad_df.empty:
    fig, ax = plt.subplots()
    ax.bar(gravedad_df["gravedad_normalizada"], gravedad_df["total_accidentes"])
    ax.set_xlabel("Gravedad")
    ax.set_ylabel("Total accidentes")
    ax.set_title("Distribución de accidentes por gravedad")
    plt.xticks(rotation=30)
    st.pyplot(fig)

    st.dataframe(gravedad_df)