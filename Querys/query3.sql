-- ¿Cuál es la comuna con más accidentes con heridos?
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