import sys
from pyspark.sql import SparkSession
from tarea1.programaestudiante.funciones.procesamiento import obtener_top_ciclistas

def main():
    if len(sys.argv) != 4:
        print("Uso: spark-submit programaestudiante/main.py <ciclistas.csv> <rutas.csv> <actividades.csv>")
        sys.exit(1)

    path_ciclistas = sys.argv[1]
    path_rutas = sys.argv[2]
    path_actividades = sys.argv[3]

    spark = SparkSession.builder.appName("Ranking Ciclistas").getOrCreate()

    # Definir los nombres de las columnas porque los archivos no tienen encabezado
    ciclistas_schema = ["cedula", "nombre", "provincia"]
    rutas_schema = ["codigo", "nombre", "kilometros"]
    actividades_schema = ["cedula", "codigo", "fecha"]

    # Leer archivos sin encabezado y asignar nombres de columnas
    ciclistas_df = spark.read.csv(path_ciclistas, inferSchema=True).toDF(*ciclistas_schema)
    rutas_df = spark.read.csv(path_rutas, inferSchema=True).toDF(*rutas_schema)
    actividades_df = spark.read.csv(path_actividades, inferSchema=True).toDF(*actividades_schema)

    # Llamar a la función que obtiene el top 5
    resultados = obtener_top_ciclistas(spark, ciclistas_df, rutas_df, actividades_df)

    # Mostrar los resultados por provincia
    for provincia, df in resultados.items():
        print(f"\n🏁 Top 5 Ciclistas de {provincia}:")
        df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
