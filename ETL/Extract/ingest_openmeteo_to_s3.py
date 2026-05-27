import os
import boto3
import requests
import pandas as pd
from datetime import datetime

BUCKET_NAME = "accidentes-medellin-proyecto"
S3_PREFIX = "raw/climaopenmeteo"

# Coordenadas de Medellín
LATITUDE = 6.2442
LONGITUDE = -75.5812

# Años requeridos por el dataset de accidentes
YEARS = range(2014, 2021)

LOCAL_DIR = "/home/ubuntu/proyecto2/clima"
os.makedirs(LOCAL_DIR, exist_ok=True)

fecha_carga = datetime.now().strftime("%Y-%m-%d")

s3 = boto3.client("s3")
s3 = boto3.client("s3")

for year in YEARS:
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={LATITUDE}"
        f"&longitude={LONGITUDE}"
        f"&start_date={start_date}"
        f"&end_date={end_date}"
        "&hourly=rain,precipitation,is_day"
        "&timezone=America%2FBogota"
    )

    print(f"Consultando Open-Meteo para el año {year}")
    print(url)

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    data = response.json()
    hourly = data["hourly"]

    df = pd.DataFrame({
        "date": hourly["time"],
        "rain": hourly["rain"],
        "precipitation": hourly["precipitation"],
        "is_day": hourly["is_day"],
        "year": year
    })

    local_file = f"{LOCAL_DIR}/clima_openmeteo_{year}.csv"
    s3_key = f"{S3_PREFIX}/year={year}/clima_openmeteo_{year}.csv"

    df.to_csv(local_file, index=False)

    s3.upload_file(local_file, BUCKET_NAME, s3_key)

    print(f"Archivo local creado: {local_file}")
    print(f"Archivo cargado en: s3://{BUCKET_NAME}/{s3_key}")
    print(f"Registros generados para {year}: {len(df)}")
    print("-------------------------------------------")

print("Ingesta Open-Meteo finalizada correctamente.")