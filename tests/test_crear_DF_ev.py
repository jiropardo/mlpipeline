from pyspark.sql import SparkSession
import tempfile
import os
from pyspark.sql.functions import col, upper, translate
from funciones.procesamiento import crear_DF_ev


def test_crear_DF_ev_csv(spark):
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "esperanzadevida.csv")

        # CSV mínimo de ejemplo
        csv_content = """Canton;2010;2011;2012;2013;2014;2015;2016;2017;2018;2019;2020
Quito;72;73;73;74;74;75;75;76;76;77;78
Guayaquil;75;76;76;77;77;78;78;79;79;80;81
"""
        with open(csv_path, "w") as f:
            f.write(csv_content)

        # Ejecutar la función
        df_result = crear_DF_ev(spark, path_Esperanzadevida=csv_path)

        # Verificar columnas
        expected_columns = ["Canton", "EsperanzaVida"]
        assert df_result.columns == expected_columns

        # Verificar resultados
        result_dict = {row.Canton: row.EsperanzaVida for row in df_result.collect()}
        assert result_dict["QUITO"] == "78"
        assert result_dict["GUAYAQUIL"] == "81"