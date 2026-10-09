"""
TALLER ETL CON PYSPARK: PROCESAMIENTO Y LIMPIEZA DE LA LIGA COLOMBIANA
Proceso ETL:
  1. Extracción: Lectura del archivo CSV 'liga_colombiana_sucia (1).csv'.
  2. Transformación: Limpieza de duplicados, normalización de nombres de equipos 
     y corrección de fechas faltantes.
  3. Carga: Exportación a Parquet y registro de vista temporal para consultas Spark SQL.
"""

import os
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, lit

def main():
    # ---------------------------------------------------------
    # PASO 1: INICIALIZACIÓN DE LA SESIÓN DE SPARK
    # ---------------------------------------------------------
    spark = SparkSession.builder \
        .appName("ETL_Liga_Colombiana") \
        .getOrCreate()

    # Silenciar logs innecesarios de la JVM
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------------------------------------------------
    # FASE 1: EXTRACCIÓN (E)
    # ---------------------------------------------------------
    DATA_PATH = "liga_colombiana_sucia (1).csv"

    # Verificación preventiva: si el archivo en Windows no tiene extensión .csv visible
    if not os.path.exists(DATA_PATH) and os.path.exists("liga_colombiana_sucia (1)"):
        DATA_PATH = "liga_colombiana_sucia (1)"

    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(DATA_PATH)

    total_inicial = df_raw.count()
    print("=" * 60)
    print(f"FASE 1 - EXTRACCIÓN: Registros leídos inicialmente: {total_inicial}")
    print("=" * 60)

    # ---------------------------------------------------------
    # FASE 2: TRANSFORMACIÓN Y LIMPIEZA (T)
    # ---------------------------------------------------------
    # Paso 2.1: Eliminación de duplicados
    df_sin_duplicados = df_raw.dropDuplicates()
    total_sin_duplicados = df_sin_duplicados.count()
    duplicados_removidos = total_inicial - total_sin_duplicados

    # Paso 2.2: Normalización de nombres de equipos (Mayúsculas y sin espacios)
    df_normalizado = df_sin_duplicados.withColumn("equipo", trim(upper(col("equipo"))))

    # Paso 2.3: Reemplazar fechas 'fecha desconocida' o nulas con la fecha predeterminada
    fecha_default = "2024-07-24 6:15 pm"
    
    df_limpio = df_normalizado.withColumn(
        "fecha",
        when(
            (trim(col("fecha")) == "fecha desconocida") | 
            (col("fecha").isNull()) | 
            (trim(col("fecha")) == ""), 
            lit(fecha_default)
        ).otherwise(trim(col("fecha")))
    )

    total_limpio = df_limpio.count()

    print("\nRESUMEN DE AUDITORÍA DE CALIDAD DE DATOS (ETL):")
    print(f"- Registros originales:          {total_inicial}")
    print(f"- Registros duplicados removidos: {duplicados_removidos}")
    print(f"- Registros finales procesados:  {total_limpio}")
    print("=" * 60)

    # ---------------------------------------------------------
    # FASE 3: CARGA (L) - EXPORTACIÓN PARQUET Y CREACIÓN DE VISTA SQL
    # ---------------------------------------------------------
    output_path = Path("liga_colombiana_limpia.parquet")
    shutil.rmtree(output_path, ignore_errors=True)
    
    df_limpio.write.mode("overwrite").parquet(str(output_path))
    print(f"\n✓ Dataset exportado exitosamente a Parquet en '{output_path}'")

    # Registro de vista temporal para consultas Spark SQL
    df_limpio.createOrReplaceTempView("liga_colombiana")

    print("\nESQUEMA DE DATOS LIMPIOS:")
    df_limpio.printSchema()

    # ---------------------------------------------------------
    # CONSULTAS SPARK SQL
    # ---------------------------------------------------------
    print("\n" + "=" * 60)
    print("           RESULTADOS DE LAS CONSULTAS SPARK SQL        ")
    print("=" * 60)

    # Pregunta 1: Equipo con más partidos
    print("\n--- PREGUNTA 1: Equipo con más partidos registrado ---")
    spark.sql("""
        SELECT equipo, COUNT(*) AS total_partidos
        FROM liga_colombiana
        GROUP BY equipo
        ORDER BY total_partidos DESC
        LIMIT 2
    """).show(truncate=False)

    # Pregunta 2: Marcador más frecuente
    print("\n--- PREGUNTA 2: Marcador (Goles Favor vs Goles Contra) más frecuente ---")
    spark.sql("""
        SELECT 
            CONCAT(CAST(COALESCE(goles_favor, 0) AS INT), ' - ', CAST(COALESCE(goles_contra, 0) AS INT)) AS marcador,
            COUNT(*) AS frecuencia
        FROM liga_colombiana
        GROUP BY marcador
        ORDER BY frecuencia DESC
        LIMIT 1
    """).show(truncate=False)

    # Pregunta 3: Promedio de goles en contra para Atlético Nacional
    print("\n--- PREGUNTA 3: Promedio de goles en contra para Atlético Nacional ---")
    spark.sql("""
        SELECT ROUND(AVG(goles_contra), 2) AS promedio_goles_contra
        FROM liga_colombiana
        WHERE equipo LIKE '%ATL%TICO NACIONAL%'
    """).show(truncate=False)

    # Pregunta 4: Fecha(s) con más goles a favor para Santa Fe
    print("\n--- PREGUNTA 4: Fecha(s) con mayor número de goles a favor para Santa Fe ---")
    spark.sql("""
        SELECT fecha, CAST(goles_favor AS INT) AS goles_a_favor
        FROM liga_colombiana
        WHERE equipo LIKE '%SANTA FE%'
          AND goles_favor = (
              SELECT MAX(goles_favor) 
              FROM liga_colombiana 
              WHERE equipo LIKE '%SANTA FE%'
          )
    """).show(truncate=False)

    # Cierre de la sesión Spark
    spark.stop()

if __name__ == "__main__":
    main()
