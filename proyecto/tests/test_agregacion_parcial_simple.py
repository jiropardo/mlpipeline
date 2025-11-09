import pytest
from pyspark.sql import SparkSession
from programaestudiante.funciones.procesamiento import agregacion_parcial
from pyspark.sql import Row

def test_agregacion_parcial(spark):
    data = [
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-01", kilometros=10.0),
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-01", kilometros=5.0),
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-02", kilometros=7.0),
        Row(cedula=2, provincia="Heredia", fecha="2023-10-01", kilometros=20.0),
    ]
    df_unido = spark.createDataFrame(data)
    df_agg = agregacion_parcial(df_unido)

    resultados = { (row.cedula, row.fecha): row.total_km for row in df_agg.collect() }

    assert resultados[(1, "2023-10-01")] == 15.0
    assert resultados[(1, "2023-10-02")] == 7.0
    assert resultados[(2, "2023-10-01")] == 20.0