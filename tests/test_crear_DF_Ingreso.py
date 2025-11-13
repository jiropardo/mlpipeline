import pytest
from pyspark.sql import SparkSession
import tempfile
import os
from pyspark.sql.functions import col, upper, translate
from funciones.procesamiento import crear_DF_Ingreso


def test_crear_DF_Ingreso_csv(spark):
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "ingreso.csv")

        # CSV mínimo de ejemplo
        csv_content = """Canton;2010;2011;2012;2013;2014;2015;2016;2017;2018;2019;2020
Quito;1000;1100;1200;1300;1400;1500;1600;1700;1800;1900;2000
Guayaquil;2000;2100;2200;2300;2400;2500;2600;2700;2800;2900;3000
"""
        with open(csv_path, "w") as f:
            f.write(csv_content)

        # Ejecutar la función
        df_result = crear_DF_Ingreso(spark, path_Ingresopromedioestimado=csv_path)

        # Verificar columnas
        expected_columns = ["Canton", "IngresoPromedio"]
        assert df_result.columns == expected_columns

        # Verificar resultados
        result_dict = {row.Canton: row.IngresoPromedio for row in df_result.collect()}
        assert result_dict["QUITO"] == "2000"
        assert result_dict["GUAYAquil".upper()] == "3000"