from programaestudiante.funciones.procesamiento import unir_datos

def test_unir_datos_mal_tipeados(spark):
    # 'cedula' y 'kilometros' con tipos incorrectos
    ciclistas_df = spark.createDataFrame([
        ("uno", "Ana", "SJ"),
    ], ["cedula", "nombre", "provincia"])  # cedula como string

    rutas_df = spark.createDataFrame([
        (10, "Ruta A", "doce"),  # kilometros como string
    ], ["codigo_ruta", "nombre_ruta", "kilometros"])

    actividades_df = spark.createDataFrame([
        (10, "uno", "2023-10-01"),  # cedula como string
    ], ["codigo_ruta", "cedula", "fecha"])

    # Debería ejecutar sin lanzar excepción, pero tipos estarán mal
    df_resultado = unir_datos(ciclistas_df, rutas_df, actividades_df)
    assert df_resultado.count() == 1
    assert isinstance(df_resultado.first().kilometros, str)
