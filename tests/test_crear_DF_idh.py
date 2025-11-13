from pyspark.sql import SparkSession
import tempfile
import os
from pyspark.sql.functions import col, upper, translate
from funciones.procesamiento import crear_DF_idh


def test_crear_DF_idh_csv(spark):
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "idh.csv")

        # CSV mínimo de ejemplo
        csv_content = """Canton;2010;2011;2012;2013;2014;2015;2016;2017;2018;2019;2020
Quito;0.7;0.71;0.72;0.73;0.74;0.75;0.76;0.77;0.78;0.79;0.80
Guayaquil;0.8;0.81;0.82;0.83;0.84;0.85;0.86;0.87;0.88;0.89;0.90
"""
        with open(csv_path, "w") as f:
            f.write(csv_content)

        # Ejecutar la función
        df_result = crear_DF_idh(spark, path_idh=csv_path)

        # Verificar columnas
        expected_columns = ["Canton", "IndiceDesarolloHumano"]
        assert df_result.columns == expected_columns

        # Verificar resultados
        result_dict = {row.Canton: row.IndiceDesarolloHumano for row in df_result.collect()}
        assert result_dict["QUITO"] == "0.80"
        assert result_dict["GUAYAQUIL"] == "0.90"