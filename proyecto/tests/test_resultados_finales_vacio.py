from programaestudiante.funciones.procesamiento import resultados_finales

def test_resultados_finales_vacio(spark):
    df_vacio = spark.createDataFrame([], "cedula INT, provincia STRING, fecha STRING, total_km DOUBLE")

    df_top = resultados_finales(df_vacio, top_n=3)

    assert df_top.count() == 0
