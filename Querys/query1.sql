-- ¿Cuál fue el año con más accidentes?
SELECT 
    anio_accidente,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
GROUP BY anio_accidente
ORDER BY total_accidentes DESC;