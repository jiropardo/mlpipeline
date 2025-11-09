from programaestudiante.funciones.procesamiento import unir_datos

def test_unir_datos_schema(spark):
    ciclistas_df = spark.createDataFrame([
        (1, "Ana", "SJ"),
    ], ["cedula", "nombre", "provincia"])

    rutas_df = spark.createDataFrame([
        (10, "Ruta A", 12.0),
    ], ["codigo_ruta", "nombre_ruta", "kilometros"])

    actividades_df = spark.createDataFrame([
        (10, 1, "2023-10-01"),
    ], ["codigo_ruta", "cedula", "fecha"])

    df_resultado = unir_datos(ciclistas_df, rutas_df, actividades_df)

    columnas_esperadas = {"cedula", "nombre", "provincia", "codigo_ruta", "nombre_ruta", "kilometros", "fecha"}
    columnas_reales = set(df_resultado.columns)

    assert columnas_esperadas.issubset(columnas_reales), f"Faltan columnas: {columnas_esperadas - columnas_reales}"

