from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, DateType
from pyspark.sql.functions import col, when
from funciones.procesamiento import crear_DF_Delitos
from io import StringIO
import tempfile


def test_crear_DF_Delitos():
    # Crear SparkSession
    spark = SparkSession.builder.master("local[1]").appName("pytest_crear_DF_Delitos").getOrCreate()

    # CSV simulado en memoria
    csv_data = """Delito,SubDelito,Fecha,Victima,SubVictima,x,Edad,Sexo,Nacionalidad,Provincia,Canton
ROBO,ROBO CON ARMA,,Persona,Adulto,1,25,M,Ecuatoriano,Pichincha,Quito
ROBO,ROBO SIN ARMA,,Persona,Adulto,2,30,F,Ecuatoriano,Pichincha,Quito
ROBO,ROBO CON ARMA,,Persona,Adulto,3,45,M,Ecuatoriano,Guayas,Guayaquil
"""

    # Guardar CSV temporal
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_data)
        temp_path = f.name

    # Ejecutar la función con el path temporal
    result_df = crear_DF_Delitos(spark, temp_path)

    # Convertir a lista de diccionarios para verificar
    result_data = [row.asDict() for row in result_df.collect()]

    # Columnas esperadas
    expected_cols = [
        "ASALTO_ARMADO",
        "Provincia",
        "Canton",
        "Victima",
        "Edad_Victima",
        "Sexo_Victima",
        "Total_Asaltos"
    ]
    assert all(c in result_df.columns for c in expected_cols)

    # Contar filas según ASALTO_ARMADO
    asaltos_agrupados = {}
    for r in result_data:
        key = r["ASALTO_ARMADO"]
        asaltos_agrupados[key] = asaltos_agrupados.get(key, 0) + 1

    # Verificar combinaciones únicas según la salida de la función
    assert asaltos_agrupados.get("SI", 0) == 2  # filas únicas con ASALTO_ARMADO="SI"
    assert asaltos_agrupados.get("NO", 0) == 1  # filas únicas con ASALTO_ARMADO="NO"

    # Cerrar Spark
    spark.stop()