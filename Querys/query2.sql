--¿Cuál es la comuna o corregimiento con más accidentes?
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