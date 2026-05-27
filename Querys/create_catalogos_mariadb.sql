-- ==========================================================
-- Proyecto Big Data - Accidentes Medellín + Clima
-- Script SQL para crear y poblar la base de datos MariaDB
-- Autor: Proyecto académico
-- Motor: MariaDB / MySQL
-- ==========================================================

-- ==========================================================
-- 1. Creación de base de datos
-- ==========================================================

CREATE DATABASE IF NOT EXISTS accidentalidad_medellin
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE accidentalidad_medellin;

-- ==========================================================
-- 2. Limpieza opcional de tablas existentes
-- ==========================================================
-- Ejecutar esta sección solo si se quiere reconstruir todo desde cero.

DROP TABLE IF EXISTS franjas_horarias;
DROP TABLE IF EXISTS clasificacion_lluvia;
DROP TABLE IF EXISTS gravedad_accidente;
DROP TABLE IF EXISTS tipo_accidente;
DROP TABLE IF EXISTS comunas;

-- ==========================================================
-- 3. Tabla: comunas
-- ==========================================================
-- Esta tabla sirve como catálogo territorial.
-- Incluye las 16 comunas urbanas de Medellín y los 5 corregimientos.
-- En el CSV original, los corregimientos pueden aparecer codificados como:
-- 50, 60, 70, 80 y 90.

CREATE TABLE comunas (
    id_comuna INT PRIMARY KEY,
    nombre_comuna VARCHAR(100) NOT NULL,
    zona VARCHAR(100) NOT NULL
);

INSERT INTO comunas (id_comuna, nombre_comuna, zona)
VALUES
(1, 'POPULAR', 'NORORIENTAL'),
(2, 'SANTA CRUZ', 'NORORIENTAL'),
(3, 'MANRIQUE', 'NORORIENTAL'),
(4, 'ARANJUEZ', 'NORORIENTAL'),
(5, 'CASTILLA', 'NOROCCIDENTAL'),
(6, 'DOCE DE OCTUBRE', 'NOROCCIDENTAL'),
(7, 'ROBLEDO', 'NOROCCIDENTAL'),
(8, 'VILLA HERMOSA', 'CENTRORIENTAL'),
(9, 'BUENOS AIRES', 'CENTRORIENTAL'),
(10, 'LA CANDELARIA', 'CENTRO'),
(11, 'LAURELES ESTADIO', 'CENTROCCIDENTAL'),
(12, 'LA AMERICA', 'CENTROCCIDENTAL'),
(13, 'SAN JAVIER', 'CENTROCCIDENTAL'),
(14, 'EL POBLADO', 'SURORIENTAL'),
(15, 'GUAYABAL', 'SUROCCIDENTAL'),
(16, 'BELEN', 'SUROCCIDENTAL'),

-- Corregimientos de Medellín
(50, 'SAN SEBASTIAN DE PALMITAS', 'CORREGIMIENTO'),
(60, 'SAN CRISTOBAL', 'CORREGIMIENTO'),
(70, 'ALTAVISTA', 'CORREGIMIENTO'),
(80, 'SAN ANTONIO DE PRADO', 'CORREGIMIENTO'),
(90, 'SANTA ELENA', 'CORREGIMIENTO');

-- ==========================================================
-- 4. Tabla: tipo_accidente
-- ==========================================================
-- Catálogo para normalizar y clasificar la columna CLASE_ACCIDENTE.
-- Se incluyen variantes con y sin tilde encontradas en el CSV original.
-- El campo es_valido permite excluir encabezados repetidos o valores vacíos.

CREATE TABLE tipo_accidente (
    id_tipo INT PRIMARY KEY AUTO_INCREMENT,
    clase_accidente_original VARCHAR(100),
    clase_accidente_normalizada VARCHAR(100),
    categoria_general VARCHAR(100),
    nivel_riesgo VARCHAR(50),
    es_valido TINYINT DEFAULT 1
);

INSERT INTO tipo_accidente
(clase_accidente_original, clase_accidente_normalizada, categoria_general, nivel_riesgo, es_valido)
VALUES
('Choque', 'CHOQUE', 'COLISION', 'MEDIO', 1),
('Atropello', 'ATROPELLO', 'PERSONA INVOLUCRADA', 'ALTO', 1),
('Volcamiento', 'VOLCAMIENTO', 'PERDIDA DE CONTROL', 'ALTO', 1),
('Incendio', 'INCENDIO', 'EVENTO ESPECIAL', 'ALTO', 1),
('Otro', 'OTRO', 'OTRO', 'BAJO', 1),

-- Variantes de caída ocupante
('Caida Ocupante', 'CAIDA OCUPANTE', 'PERDIDA DE CONTROL', 'MEDIO', 1),
('Caída Ocupante', 'CAIDA OCUPANTE', 'PERDIDA DE CONTROL', 'MEDIO', 1),
('Caida de Ocupante', 'CAIDA OCUPANTE', 'PERDIDA DE CONTROL', 'MEDIO', 1),
('Caída de Ocupante', 'CAIDA OCUPANTE', 'PERDIDA DE CONTROL', 'MEDIO', 1),

-- Valores inválidos o no informados detectados en el CSV
('', 'NO INFORMADO', 'NO INFORMADO', 'NO INFORMADO', 0),
('CLASE_ACCIDENTE', 'NO VALIDO', 'ENCABEZADO REPETIDO', 'NO VALIDO', 0);

-- ==========================================================
-- 5. Tabla: gravedad_accidente
-- ==========================================================
-- Catálogo para normalizar la columna GRAVEDAD_ACCIDENTE.
-- El campo severidad_numerica permite análisis ordenado:
-- 1 = solo daños, 2 = heridos, 3 = muertos.

CREATE TABLE gravedad_accidente (
    id_gravedad INT PRIMARY KEY AUTO_INCREMENT,
    gravedad_original VARCHAR(100),
    gravedad_normalizada VARCHAR(100),
    severidad_numerica INT,
    descripcion VARCHAR(255),
    es_valido TINYINT DEFAULT 1
);

INSERT INTO gravedad_accidente
(gravedad_original, gravedad_normalizada, severidad_numerica, descripcion, es_valido)
VALUES
('Solo daños', 'SOLO DANOS', 1, 'Accidente con daños materiales', 1),
('Solo da\\xF1os', 'SOLO DANOS', 1, 'Accidente con daños materiales', 1),
('Solo daÃ±os', 'SOLO DANOS', 1, 'Accidente con daños materiales', 1),
('Con heridos', 'CON HERIDOS', 2, 'Accidente con una o más personas lesionadas', 1),
('Con muertos', 'CON MUERTOS', 3, 'Accidente con una o más víctimas fatales', 1),
('GRAVEDAD_ACCIDENTE', 'NO VALIDO', 0, 'Encabezado repetido o fila no válida', 0);

-- ==========================================================
-- 6. Tabla: clasificacion_lluvia
-- ==========================================================
-- Catálogo para clasificar la lluvia acumulada diaria en milímetros.
-- Esta clasificación se aplica sobre lluvia_total_dia, no sobre lluvia horaria.
--
-- Rangos recomendados:
-- 0.00 mm      = SIN LLUVIA
-- 0.01-10.00  = LLUVIA LIGERA
-- 10.01-30.00 = LLUVIA MODERADA
-- 30.01-70.00 = LLUVIA FUERTE
-- >70.00      = LLUVIA MUY FUERTE

CREATE TABLE clasificacion_lluvia (
    id_clasificacion INT PRIMARY KEY AUTO_INCREMENT,
    lluvia_min DECIMAL(6,2) NOT NULL,
    lluvia_max DECIMAL(6,2) NOT NULL,
    categoria_lluvia VARCHAR(50) NOT NULL
);

INSERT INTO clasificacion_lluvia
(lluvia_min, lluvia_max, categoria_lluvia)
VALUES
(0.00, 0.00, 'SIN LLUVIA'),
(0.01, 10.00, 'LLUVIA LIGERA'),
(10.01, 30.00, 'LLUVIA MODERADA'),
(30.01, 70.00, 'LLUVIA FUERTE'),
(70.01, 999.00, 'LLUVIA MUY FUERTE');

-- ==========================================================
-- 7. Tabla: franjas_horarias
-- ==========================================================
-- Catálogo para clasificar horas del día.
-- Aunque el análisis principal cruza clima a nivel diario, esta tabla queda
-- disponible para un análisis futuro por hora si se conserva fecha_accidente_ts.

CREATE TABLE franjas_horarias (
    id_franja INT PRIMARY KEY AUTO_INCREMENT,
    hora_inicio INT NOT NULL,
    hora_fin INT NOT NULL,
    franja VARCHAR(50) NOT NULL
);

INSERT INTO franjas_horarias
(hora_inicio, hora_fin, franja)
VALUES
(0, 5, 'MADRUGADA'),
(6, 11, 'MAÑANA'),
(12, 17, 'TARDE'),
(18, 23, 'NOCHE');

-- ==========================================================
-- 8. Validaciones rápidas
-- ==========================================================

SELECT 'comunas' AS tabla, COUNT(*) AS total_registros FROM comunas
UNION ALL
SELECT 'tipo_accidente' AS tabla, COUNT(*) AS total_registros FROM tipo_accidente
UNION ALL
SELECT 'gravedad_accidente' AS tabla, COUNT(*) AS total_registros FROM gravedad_accidente
UNION ALL
SELECT 'clasificacion_lluvia' AS tabla, COUNT(*) AS total_registros FROM clasificacion_lluvia
UNION ALL
SELECT 'franjas_horarias' AS tabla, COUNT(*) AS total_registros FROM franjas_horarias;

-- Validar comunas y corregimientos
SELECT *
FROM comunas
ORDER BY id_comuna;

-- Validar rangos de lluvia
SELECT *
FROM clasificacion_lluvia
ORDER BY lluvia_min;

-- Validar tipos de accidente válidos
SELECT *
FROM tipo_accidente
WHERE es_valido = 1
ORDER BY id_tipo;

-- Validar gravedades válidas
SELECT *
FROM gravedad_accidente
WHERE es_valido = 1
ORDER BY severidad_numerica;

-- ==========================================================
-- 9. Script de actualización idempotente para corregimientos
-- ==========================================================
-- Si la tabla comunas ya existe y solo se quieren asegurar los corregimientos,
-- se puede ejecutar este bloque sin reconstruir toda la base.

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

-- ==========================================================
-- 10. Script de actualización idempotente para clasificación de lluvia
-- ==========================================================
-- Si solo se quiere actualizar el catálogo de lluvia diaria acumulada,
-- ejecutar este bloque.

TRUNCATE TABLE clasificacion_lluvia;

INSERT INTO clasificacion_lluvia
(lluvia_min, lluvia_max, categoria_lluvia)
VALUES
(0.00, 0.00, 'SIN LLUVIA'),
(0.01, 10.00, 'LLUVIA LIGERA'),
(10.01, 30.00, 'LLUVIA MODERADA'),
(30.01, 70.00, 'LLUVIA FUERTE'),
(70.01, 999.00, 'LLUVIA MUY FUERTE');

-- ==========================================================
-- Fin del script
-- ==========================================================
