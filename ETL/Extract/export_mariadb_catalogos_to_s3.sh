#!/bin/bash

set -e

source "$HOME/proyecto2/scripts/mariadb_config.env"

FECHA_CARGA=$(date +%Y-%m-%d)
LOCAL_DIR="$HOME/proyecto2/catalogos_mariadb"
LOG_FILE="$HOME/proyecto2/logs/export_mariadb_catalogos.log"

mkdir -p "$LOCAL_DIR"

echo "======================================" >> "$LOG_FILE"
echo "Inicio exportación MariaDB: $(date)" >> "$LOG_FILE"

TABLAS=(
  "comunas"
  "tipo_accidente"
  "gravedad_accidente"
  "clasificacion_lluvia"
  "franjas_horarias"
)

for TABLA in "${TABLAS[@]}"
do
  echo "Exportando tabla: $TABLA" >> "$LOG_FILE"

  mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" \
    --batch \
    --raw \
    -e "SELECT * FROM $TABLA;" \
    | sed 's/\t/,/g' > "$LOCAL_DIR/${TABLA}.csv"

  aws s3 cp "$LOCAL_DIR/${TABLA}.csv" \
    "$S3_BUCKET/raw/catalogo_mariaDB/fecha_carga=$FECHA_CARGA/${TABLA}.csv"

  echo "Tabla $TABLA cargada a S3" >> "$LOG_FILE"
done

echo "Fin exportación MariaDB: $(date)" >> "$LOG_FILE"