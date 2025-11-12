from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, DateType
from pyspark.sql.functions import col, when

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql import Row
import datetime
import tempfile
import os
from funciones.procesamiento import crear_DF_Educacion


def test_crear_DF_Educacion_csv(spark):
    # Crear archivos CSV temporales para edad y sexo
    with tempfile.TemporaryDirectory() as tmpdir:
        path_edad = os.path.join(tmpdir, "educacion_edad.csv")
        path_sexo = os.path.join(tmpdir, "educacion_sexo.csv")

        # CSV de ejemplo: educación por edad
        csv_edad = """Nivel_Educativo;Rango_Edad;Canton;Distrito;Provincia;IsGrandTotalRowTotal;Numero_Personas
Primaria;6-12;Quito;Distrito1;Pichincha;False;100
Secundaria;13-18;Quito;Distrito1;Pichincha;False;50
"""
        with open(path_edad, "w") as f:
            f.write(csv_edad)

        # CSV de ejemplo: educación por sexo
        csv_sexo = """Nivel_Educativo;Sexo;Canton;Distrito;Provincia;IsGrandTotalRowTotal;DistinctCountsnb003_id_persona
Primaria;M;Quito;Distrito1;Pichincha;False;55
Primaria;F;Quito;Distrito1;Pichincha;False;45
Secundaria;M;Quito;Distrito1;Pichincha;False;25
Secundaria;F;Quito;Distrito1;Pichincha;False;25
"""
        with open(path_sexo, "w") as f:
            f.write(csv_sexo)

        # Ejecutar la función
        df_result = crear_DF_Educacion(spark, path_Educacionedad=path_edad, path_Educacionsexo=path_sexo)

        # Verificar columnas
        expected_columns = ["Nivel_Educativo", "Canton", "Provincia", "Sexo", "Total_Personas_Canton"]
        assert df_result.columns == expected_columns

        # Verificar resultados
        result_dict = { (row.Nivel_Educativo, row.Canton, row.Provincia, row.Sexo): row.Total_Personas_Canton
                        for row in df_result.collect() }

        assert result_dict[("Primaria", "Quito", "Pichincha", "M")] == 55
        assert result_dict[("Primaria", "Quito", "Pichincha", "F")] == 45
        assert result_dict[("Secundaria", "Quito", "Pichincha", "M")] == 25
        assert result_dict[("Secundaria", "Quito", "Pichincha", "F")] == 25