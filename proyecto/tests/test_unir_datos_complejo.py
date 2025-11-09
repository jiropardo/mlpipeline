import pytest
from pyspark.sql import Row
from programaestudiante.funciones.procesamiento import unir_datos

def test_unir_datos_complejo(spark):
    # Datos de ciclistas: Ana tiene actividades, Luis no
    ciclistas = [
        Row(cedula=1, nombre="Ana", provincia="Alajuela"),
        Row(cedula=2, nombre="Luis", provincia="Heredia"),
    ]
    ciclistas_df = spark.createDataFrame(ciclistas)

    # Datos de rutas
    rutas = [
        Row(codigo_ruta=100, nombre_ruta="Ruta A", kilometros=10.0),
        Row(codigo_ruta=200, nombre_ruta="Ruta B", kilometros=20.0),
    ]
    rutas_df = spark.createDataFrame(rutas)

    # Actividades solo para Ana
    actividades = [
        Row(codigo_ruta=100, cedula=1, fecha="2023-10-01"),
        Row(codigo_ruta=100, cedula=1, fecha="2023-10-02"),
        Row(codigo_ruta=200, cedula=1, fecha="2023-10-03"),
    ]
    actividades_df = spark.createDataFrame(actividades)

    # Ejecutar unión
    df_unido = unir_datos(ciclistas_df, rutas_df, actividades_df)
    df_unido.cache()

    # ✅ Validación para Ana (3 actividades con datos completos)
    filas_ana = df_unido.filter("cedula = 1").collect()
    assert len(filas_ana) == 3, "Ana debe tener 3 filas en unión"
    assert all(f.codigo_ruta in (100, 200) for f in filas_ana), "Las rutas deben ser 100 o 200"

    # ✅ Validación para Luis (sin actividad → valores nulos)
    fila_luis = df_unido.filter("cedula = 2").collect()
    assert len(fila_luis) == 1, "Luis debe aparecer con una fila aunque no tenga actividad"
    assert fila_luis[0].codigo_ruta is None
    assert fila_luis[0].nombre_ruta is None
    assert fila_luis[0].kilometros is None
    assert fila_luis[0].fecha is None
