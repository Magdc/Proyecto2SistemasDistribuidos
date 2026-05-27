-- ¿Cuántos accidentes ocurren según la categoría de lluvia?
SELECT 
    COALESCE(categoria_lluvia, 'SIN DATO CLIMA') AS categoria_lluvia,
    COUNT(*) AS total_accidentes
FROM db_accidentes_medellin.accidentes_enriquecidos
GROUP BY COALESCE(categoria_lluvia, 'SIN DATO CLIMA')
ORDER BY total_accidentes DESC;