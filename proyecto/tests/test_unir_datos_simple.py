from programaestudiante.funciones.procesamiento import unir_datos

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

def test_unir_datos(spark):
    ciclistas = [
        Row(cedula=1, nombre="Ana", provincia="Alajuela"),
        Row(cedula=2, nombre="Luis", provincia="Heredia")
    ]
    ciclistas_df = spark.createDataFrame(ciclistas)

    rutas = [
        Row(codigo_ruta=100, nombre_ruta="Ruta A", kilometros=10.0),
        Row(codigo_ruta=200, nombre_ruta="Ruta B", kilometros=20.0),
    ]
    rutas_df = spark.createDataFrame(rutas)

    actividades = [
        Row(codigo_ruta=100, cedula=1, fecha="2023-10-01"),
        Row(codigo_ruta=100, cedula=1, fecha="2023-10-02"),
        Row(codigo_ruta=200, cedula=1, fecha="2023-10-03"),
    ]
    actividades_df = spark.createDataFrame(actividades)

    df_unido = unir_datos(ciclistas_df, rutas_df, actividades_df)
    df_unido.cache()

    filas_ana = df_unido.filter("cedula = 1").collect()
    assert len(filas_ana) == 3
    assert all(f.codigo_ruta in (100, 200) for f in filas_ana)

    fila_luis = df_unido.filter("cedula = 2").collect()
    assert len(fila_luis) == 1
    assert fila_luis[0]["codigo_ruta"] is None
    assert fila_luis[0]["nombre_ruta"] is None
    assert fila_luis[0]["kilometros"] is None
    assert fila_luis[0]["fecha"] is None
