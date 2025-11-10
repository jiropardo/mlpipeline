import pytest
from pyspark.sql import Row
from programaestudiante.funciones.procesamiento import agregacion_parcial

def test_agregacion_parcial_avanzado(spark):
    data = [
        # Ciclista 1 en Alajuela, 2 fechas, varios registros incluyendo nulos
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-01", kilometros=10.0),
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-01", kilometros=5.0),
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-01", kilometros=None),   # Nulo km no suma
        Row(cedula=1, provincia="Alajuela", fecha="2023-10-02", kilometros=7.0),

        # Ciclista 2 en Heredia, 1 fecha
        Row(cedula=2, provincia="Heredia", fecha="2023-10-01", kilometros=20.0),

        # Ciclista 3 en San Jose, 2 fechas, km nulos y cero
        Row(cedula=3, provincia="San Jose", fecha="2023-10-01", kilometros=0.0),
        Row(cedula=3, provincia="San Jose", fecha="2023-10-01", kilometros=15.0),
        Row(cedula=3, provincia="San Jose", fecha="2023-10-02", kilometros=None),
        Row(cedula=3, provincia="San Jose", fecha="2023-10-02", kilometros=5.0),

        # Ciclista 4 en Alajuela, fecha nula - debería agruparse con fecha nula
        Row(cedula=4, provincia="Alajuela", fecha=None, kilometros=12.0),
        Row(cedula=4, provincia="Alajuela", fecha=None, kilometros=3.0),

        # Ciclista 5 con provincia nula - también debe procesar
        Row(cedula=5, provincia=None, fecha="2023-10-01", kilometros=8.0),
    ]

    df_unido = spark.createDataFrame(data)
    df_agg = agregacion_parcial(df_unido)

    resultados = {(row.cedula, row.provincia, row.fecha): row.total_km for row in df_agg.collect()}

    # Validaciones explícitas:

    # Ciclista 1, Alajuela
    assert pytest.approx(resultados[(1, "Alajuela", "2023-10-01")], 0.001) == 15.0  # 10 + 5 + None (ignorado)
    assert pytest.approx(resultados[(1, "Alajuela", "2023-10-02")], 0.001) == 7.0

    # Ciclista 2, Heredia
    assert pytest.approx(resultados[(2, "Heredia", "2023-10-01")], 0.001) == 20.0

    # Ciclista 3, San Jose
    assert pytest.approx(resultados[(3, "San Jose", "2023-10-01")], 0.001) == 15.0  # 0 + 15
    assert pytest.approx(resultados[(3, "San Jose", "2023-10-02")], 0.001) == 5.0   # None ignorado

    # Ciclista 4, Alajuela, fecha nula
    assert pytest.approx(resultados[(4, "Alajuela", None)], 0.001) == 15.0  # 12 + 3

    # Ciclista 5, provincia nula
    assert pytest.approx(resultados[(5, None, "2023-10-01")], 0.001) == 8.0

    # Verificamos que no hay más filas que las esperadas
    assert len(resultados) == 7  # 2+1+2+1+1 = 7 agrupaciones

    # Verificamos que ningún total_km sea None o negativo
    for v in resultados.values():
        assert v is not None
        assert v >= 0

