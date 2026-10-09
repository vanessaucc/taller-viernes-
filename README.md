from pyspark.sql import SparkSession

# Aquí encendemos el motor y le ponemos un nombre a nuestro proyecto
spark = SparkSession.builder \
    .appName("ETL_NYC_Flights_2013") \
    .getOrCreate()

# Cargar la tabla principal de vuelos
df_flights = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("flights.csv")

# Cargar el catálogo de aerolíneas
df_airlines = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("airlines.csv")
Vanessa Mena jejejejje
# Cargar el catálogo de aeropuertos
df_airports = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") 
    .load("airports.csv")

# Cargar el catálogo de aviones
df_planes = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("planes.csv")

# Ver las columnas y sus tipos de datos
df_flights.printSchema()

# Ver las primeras 5 filas de la tabla de vuelos
df_flights.show(5)




Sjdjsjsbsjkajansnssosbd




import pandas as pd

# 1. Cargar el dataset original
input_csv = "liga_colombiana_sucia (1).csv"
output_parquet = "liga_colombiana_limpia.parquet"

df = pd.read_csv(input_csv)

# ==========================================
# TRANSFORMACIONES (ETL)
# ==========================================

# • Eliminar duplicados
df_clean = df.drop_duplicates().copy()

# • Normalizar los nombres de los equipos, convertir a mayúsculas y eliminar espacios extras
df_clean["equipo"] = df_clean["equipo"].astype(str).str.strip().str.upper()

# • Regenerar fechas faltantes o "fecha desconocida" reemplazando con la fecha predeterminada '2024-07-24 6:15 pm'
fecha_predeterminada = "2024-07-24 6:15 pm"
df_clean["fecha"] = df_clean["fecha"].astype(str).str.strip()
df_clean["fecha"] = df_clean["fecha"].replace(
    {
        "fecha desconocida": fecha_predeterminada,
        "": fecha_predeterminada,
        "nan": fecha_predeterminada,
        "None": fecha_predeterminada,
    }
).fillna(fecha_predeterminada)

# • Guardar el dataset en formato Parquet
# Nota: Requiere tener instalado 'pyarrow' o 'fastparquet' en el entorno virtual
df_clean.to_parquet(output_parquet, index=False)
print(f"Dataset limpio guardado exitosamente en '{output_parquet}'.\n")

# ==========================================
# CONSULTAS
# ==========================================

print("=== RESPUESTAS A LAS CONSULTAS ===\n")

# 1. ¿Cuál es el equipo con más partidos registrado en el dataset?
partidos_por_equipo = df_clean["equipo"].value_counts()
equipo_mas_partidos = partidos_por_equipo.index[0]
total_partidos = partidos_por_equipo.iloc[0]

print(f"1. Equipo con más partidos registrados: {equipo_mas_partidos} ({total_partidos} partidos)")

# 2. ¿Cuál es el marcador goles a favor vs goles en contra más frecuente en el dataset?
df_clean["goles_favor_int"] = df_clean["goles_favor"].fillna(0).astype(int)
df_clean["goles_contra_int"] = df_clean["goles_contra"].fillna(0).astype(int)
df_clean["marcador"] = (
    df_clean["goles_favor_int"].astype(str) + " - " + df_clean["goles_contra_int"].astype(str)
)

marcador_mas_frecuente = df_clean["marcador"].value_counts().index[0]
frecuencia_marcador = df_clean["marcador"].value_counts().iloc[0]

print(f"2. Marcador más frecuente (Goles Favor vs Goles Contra): {marcador_mas_frecuente} ({frecuencia_marcador} veces)")

# 3. ¿Cuál es el promedio de goles en contra para Atlético Nacional?
nacional_df = df_clean[df_clean["equipo"].str.contains("ATLÉTICO NACIONAL|ATLETICO NACIONAL", regex=True)]
promedio_gc_nacional = nacional_df["goles_contra"].mean()

print(f"3. Promedio de goles en contra para Atlético Nacional: {promedio_gc_nacional:.2f}")

# 4. ¿En qué fecha Santa Fe registró mayor número de goles a favor?
santafe_df = df_clean[df_clean["equipo"].str.contains("SANTA FE", regex=True)]
max_goles_santafe = santafe_df["goles_favor"].max()
fechas_max_goles = santafe_df[santafe_df["goles_favor"] == max_goles_santafe]["fecha"].tolist()

print(f"4. Santa Fe registró su mayor número de goles a favor ({int(max_goles_santafe)}) en la(s) fecha(s):")
for f in fechas_max_goles:
    print(f"   - {f}")
