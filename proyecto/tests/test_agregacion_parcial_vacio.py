from programaestudiante.funciones.procesamiento import agregacion_parcial

def test_agregacion_parcial_vacio(spark):
    df_vacio = spark.createDataFrame([], "cedula INT, provincia STRING, fecha STRING, kilometros DOUBLE")

    df_agg = agregacion_parcial(df_vacio)

    assert df_agg.count() == 0
