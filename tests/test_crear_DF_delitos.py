from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, DateType
from pyspark.sql.functions import col, when
from funciones.procesamiento import crear_DF_Delitos

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from funciones.procesamiento import crear_DF_Delitos  # Ajusta la importación según tu proyecto
from pyspark.sql import Row
import datetime


def test_crear_DF_Delitos_csv(spark):
    # Crear un CSV temporal
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "asaltos.csv")
        
        # Contenido mínimo del CSV con header
        csv_content = """Delito,SubDelito,Fecha,Victima,SubVictima,x,Edad,Sexo,Nacionalidad,Provincia,Canton
Robo,Robo con ARMA de fuego,2025-01-01,Persona,Adulto,1,30,M,EC,Pichincha,Quito
Robo,Robo simple,2025-01-02,Persona,Adulto,2,25,F,EC,Pichincha,Quito
"""
        with open(csv_path, "w") as f:
            f.write(csv_content)

        # Ejecutar la función
        df_result = crear_DF_Delitos(spark, path_AsaltosUltimoAnio=csv_path)

        # Verificar columnas
        expected_columns = ["ASALTO_ARMADO", "Provincia", "Canton", "Victima", "Edad_Victima", "Sexo_Victima", "Total_Asaltos"]
        assert df_result.columns == expected_columns

        # Verificar valores de ASALTO_ARMADO
        result_dict = { (row.Victima, row.Edad_Victima, row.Sexo_Victima): row.ASALTO_ARMADO for row in df_result.collect() }
        assert result_dict[("Adulto","30","M")] == "SI"
        assert result_dict[("Adulto","25","F")] == "NO"