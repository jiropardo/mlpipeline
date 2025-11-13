import pytest
from pyspark.sql import SparkSession
import tempfile
import os
from pyspark.sql.functions import col, upper, translate
from funciones.procesamiento import crear_DF_ibm


def test_crear_DF_ibm_csv(spark):
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "ibm.csv")

        # CSV mínimo de ejemplo
        csv_content = """Canton;2010;2011;2012;2013;2014;2015;2016;2017;2018;2019;2020
Quito;50;51;52;53;54;55;56;57;58;59;60
Guayaquil;60;61;62;63;64;65;66;67;68;69;70
"""
        with open(csv_path, "w") as f:
            f.write(csv_content)

        # Ejecutar la función
        df_result = crear_DF_ibm(spark, path_IBM=csv_path)

        # Verificar columnas
        expected_columns = ["Canton", "IndiceBienestarMaterial"]
        assert df_result.columns == expected_columns

        # Verificar resultados
        result_dict = {row.Canton: row.IndiceBienestarMaterial for row in df_result.collect()}
        assert result_dict["QUITO"] == "60"
        assert result_dict["GUAYAQUIL"] == "70"