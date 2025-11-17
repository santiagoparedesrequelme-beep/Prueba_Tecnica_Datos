# Ejercicio # 1 

import sqlite3

conn = sqlite3.connect('test.db')

#### PROGRAME SU RESPUESTA DENTRO DEL CONN.EXECUTE ENTRE LAS COMILLAS

cursor = conn.execute('''
WITH PartidosDesglosados AS (
    SELECT
        grupo,
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(SUBSTR(partido, 1, INSTR(partido, ' vs.') - 1))), '.', ''), 
            'á', 'A'), 'é', 'E'), 'í', 'I'), 'ó', 'O'), 'ú', 'U'), 
            'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U'), 'ñ', 'N'), 'Ñ', 'N') AS EquipoLocal,
            
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(SUBSTR(partido, INSTR(partido, 'vs. ') + 4))), '.', ''), 
            'á', 'A'), 'é', 'E'), 'í', 'I'), 'ó', 'O'), 'ú', 'U'), 
            'Á', 'A'), 'É', 'E'), 'Í', 'I'), 'Ó', 'O'), 'Ú', 'U'), 'ñ', 'N'), 'Ñ', 'N') AS EquipoVisitante,
        
        CAST(SUBSTR(resultado, 1, INSTR(resultado, ',') - 1) AS INTEGER) AS GolesLocal,
        CAST(SUBSTR(resultado, INSTR(resultado, ',') + 1) AS INTEGER) AS GolesVisitante
    FROM Partidos
),

EstadisticasPorPartido AS (
    SELECT
        grupo,
        EquipoLocal AS Equipo,
        (GolesLocal - GolesVisitante) AS Gol_Diferencia,
        CASE
            WHEN GolesLocal > GolesVisitante THEN 3
            WHEN GolesLocal = GolesVisitante THEN 1
            ELSE 0
        END AS Puntos
    FROM PartidosDesglosados

    UNION ALL

    SELECT
        grupo,
        EquipoVisitante AS Equipo,
        (GolesVisitante - GolesLocal) AS Gol_Diferencia,
        CASE
            WHEN GolesVisitante > GolesLocal THEN 3
            WHEN GolesVisitante = GolesLocal THEN 1
            ELSE 0
        END AS Puntos
    FROM PartidosDesglosados
)

SELECT
    Equipo,
    grupo,
    SUM(Puntos) AS Total_Puntos_Obtenidos,
    SUM(Gol_Diferencia) AS Sumatoria_Gol_Diferencia
FROM EstadisticasPorPartido
GROUP BY Equipo, grupo
ORDER BY grupo, Total_Puntos_Obtenidos DESC, Sumatoria_Gol_Diferencia DESC;
''') 

for row in cursor:
  print(row)
conn.close()