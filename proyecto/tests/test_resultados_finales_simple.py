import pytest
from pyspark.sql import SparkSession
from programaestudiante.funciones.procesamiento import resultados_finales
from pyspark.sql import Row

def test_resultados_finales(spark):
    data = [
        Row(cedula=116580339, provincia="Alajuela", fecha="2023-10-01", total_km=10.0),
        Row(cedula=116580339, provincia="Alajuela", fecha="2023-10-02", total_km=20.0),
        Row(cedula=116580310, provincia="Alajuela", fecha="2023-10-01", total_km=15.0),
        Row(cedula=116580350, provincia="Heredia", fecha="2023-10-01", total_km=25.0),
        Row(cedula=116580360, provincia="Heredia", fecha="2023-10-02", total_km=5.0), 
        Row(cedula=116580361, provincia="San Jose", fecha="2023-10-01", total_km=30.0), 
    ]
    df_agg = spark.createDataFrame(data)
    top_n = 5

    df_final = resultados_finales(df_agg, top_n)
    resultados = { (row.cedula, row.provincia): (row.total_km, row.promedio_diario) for row in df_final.collect() }

    assert resultados[(116580339, "Alajuela")][0] == 30.0  
    assert resultados[(116580339, "Alajuela")][1] == 15.0  
    assert resultados[(116580310, "Alajuela")][0] == 15.0
    assert resultados[(116580310, "Alajuela")][1] == 15.0 

    assert resultados[(116580350, "Heredia")][0] == 25.0
    assert resultados[(116580350, "Heredia")][1] == 25.0
    assert resultados[(116580360, "Heredia")][0] == 5.0
    assert resultados[(116580360, "Heredia")][1] == 5.0

    assert resultados[(116580361, "San Jose")][0] == 30.0
    assert resultados[(116580361, "San Jose")][1] == 30.0