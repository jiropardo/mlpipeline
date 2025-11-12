from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, DateType
from pyspark.sql.functions import col, when
from funciones.procesamiento import crear_DF_Delitos
from io import StringIO
import tempfile

def test_crear_DF_Delitos():
    spark = SparkSession.builder.master("local[1]").appName("pytest_crear_DF_Delitos").getOrCreate()

    # Simulamos un CSV en memoria
    csv_data = """Delito,SubDelito,Fecha,Victima,SubVictima,x,Edad,Sexo,Nacionalidad,Provincia,Canton
ROBO,ROBO CON ARMA,,Persona,Adulto,1,25,M,Ecuatoriano,Pichincha,Quito
ROBO,ROBO SIN ARMA,,Persona,Adulto,2,30,F,Ecuatoriano,Pichincha,Quito
ROBO,ROBO CON ARMA,,Persona,Adulto,3,45,M,Ecuatoriano,Guayas,Guayaquil
"""

    # Guardar CSV temporal
    with tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv") as f:
        f.write(csv_data)
        temp_path = f.name

    # Ejecutar función usando el path temporal
    from funciones.procesamiento import crear_DF_Delitos
    result_df = crear_DF_Delitos(spark, temp_path)

    # Verificaciones simples
    result_data = [row.asDict() for row in result_df.collect()]
    expected_cols = ["ASALTO_ARMADO", "Provincia", "Canton", "Victima", "Edad_Victima", "Sexo_Victima", "Total_Asaltos"]
    assert all(c in result_df.columns for c in expected_cols)
    assert len([r for r in result_data if r["ASALTO_ARMADO"] == "SI"]) == 2
    assert len([r for r in result_data if r["ASALTO_ARMADO"] == "NO"]) == 1

    spark.stop()
