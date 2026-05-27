#!/bin/bash

set -e

BUCKET="s3://accidentes-medellin-proyecto"
LOCAL_FILE="$HOME/proyecto2/data/incidentes_viales.csv"
FECHA_CARGA=$(date +%Y-%m-%d)
LOG_FILE="$HOME/proyecto2/logs/upload_incidentes.log"

echo "======================================" >> "$LOG_FILE"
echo "Inicio de carga: $(date)" >> "$LOG_FILE"

if [ ! -f "$LOCAL_FILE" ]; then
  echo "ERROR: No existe el archivo $LOCAL_FILE" >> "$LOG_FILE"
  exit 1
fi

aws s3 cp "$LOCAL_FILE" "$BUCKET/raw/accidentes/fecha_carga=$FECHA_CARGA/incidentes_viales.csv"

echo "Archivo cargado en $BUCKET/raw/accidentes/fecha_carga=$FECHA_CARGA/incidentes_viales.csv" >> "$LOG_>
echo "Fin de carga: $(date)" >> "$LOG_FILE"