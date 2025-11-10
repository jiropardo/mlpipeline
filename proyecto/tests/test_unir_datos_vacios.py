from programaestudiante.funciones.procesamiento import unir_datos

def test_unir_datos_vacio(spark):
    ciclistas_df = spark.createDataFrame([], "cedula INT, nombre STRING, provincia STRING")
    rutas_df = spark.createDataFrame([], "codigo_ruta INT, nombre_ruta STRING, kilometros DOUBLE")
    actividades_df = spark.createDataFrame([], "codigo_ruta INT, cedula INT, fecha STRING")

    df_resultado = unir_datos(ciclistas_df, rutas_df, actividades_df)

    assert df_resultado.count() == 0
