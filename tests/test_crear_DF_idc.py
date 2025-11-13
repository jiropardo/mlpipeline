import pytest
from pyspark.sql import SparkSession
import tempfile
import os
from pyspark.sql.functions import col, upper, translate
from funciones.procesamiento import crear_DF_idc


def test_crear_DF_idc_csv(spark):
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "idc.csv")

        # CSV mínimo de ejemplo
        csv_content = """Canton;2010;2011;2012;2013;2014;2015;2016;2017;2018;2019;2020
Quito;70;71;72;73;74;75;76;77;78;79;80
Guayaquil;80;81;82;83;84;85;86;87;88;89;90
"""
        with open(csv_path, "w") as f:
            f.write(csv_content)

        # Ejecutar la función
        df_result = crear_DF_idc(spark, path_IC=csv_path)

        # Verificar columnas
        expected_columns = ["Canton", "IndiceConocimiento"]
        assert df_result.columns == expected_columns

        # Verificar resultados
        result_dict = {row.Canton: row.IndiceConocimiento for row in df_result.collect()}
        assert result_dict["QUITO"] == "80"
        assert result_dict["GUAYAQUIL"] == "90"