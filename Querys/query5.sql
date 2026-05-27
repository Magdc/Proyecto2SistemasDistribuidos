-- ¿Qué tipos de accidente son más comunes cuando hay lluvia fuerte?
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