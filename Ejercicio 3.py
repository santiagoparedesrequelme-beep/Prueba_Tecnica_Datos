# Ejercicio # 3

import sqlite3

conn = sqlite3.connect('test.db')

####PROGRAME SU RESPUESTA DENTRO DEL CONN.EXECUTE ENTRE LAS COMILLAS

cursor = conn.execute('''
WITH Quintiles AS (
    SELECT
        equipo,
        puntos,
        gol_diferencia,
        NTILE(5) OVER (ORDER BY puntos DESC, gol_diferencia DESC) AS Quintil
    FROM Resultados_Qatar
)

SELECT
    equipo,
    puntos,
    gol_diferencia
FROM Quintiles
WHERE Quintil = 1
ORDER BY puntos DESC, gol_diferencia DESC;
                      ''')
for row in cursor:
  print(row)
conn.close()