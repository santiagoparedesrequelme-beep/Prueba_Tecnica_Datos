# Ejercicio # 2

import sqlite3

conn = sqlite3.connect('test.db')

####PROGRAME SU RESPUESTA DENTRO DEL CONN.EXECUTE ENTRE LAS COMILLAS

cursor = conn.execute('''
WITH RankingGrupal AS (
    SELECT
        equipo,
        grupo,
        puntos,
        gol_diferencia,
        RANK() OVER (PARTITION BY grupo ORDER BY puntos DESC, gol_diferencia DESC) AS Rank_Posicion
    FROM Resultados_Qatar
),

ClasificadosPorPosicion AS (
    SELECT
        T1.equipo,
        T1.grupo,
        T1.puntos,
        T1.gol_diferencia,
        CASE
            WHEN T1.Rank_Posicion = 1 THEN '1º del ' || LOWER(T1.grupo)
            WHEN T1.Rank_Posicion = 2 THEN '2º del ' || LOWER(T1.grupo)
        END AS Clave_Clasificacion
    FROM RankingGrupal T1
    WHERE T1.Rank_Posicion <= 2
)

SELECT
    C.equipo,
    C.grupo,
    C.puntos AS Total_Puntos,
    C.gol_diferencia AS Gol_Diferencia,
    COALESCE(M1.fecha, M2.fecha) AS Fecha_Juego,
    COALESCE(M1.hora, M2.hora) AS Hora_Juego,
    COALESCE(M1.sede, M2.sede) AS Sede_Ciudad,
    COALESCE(M1.partido, M2.partido) AS Partido_Siguiente_Ronda

FROM ClasificadosPorPosicion C

LEFT JOIN Clasificados M1 
    ON M1.partido LIKE C.Clave_Clasificacion || ' vs. %'

LEFT JOIN Clasificados M2
    ON M2.partido LIKE '% vs. ' || C.Clave_Clasificacion

ORDER BY C.grupo, C.puntos DESC;
''')

for row in cursor:
    print(row)
conn.close()