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
