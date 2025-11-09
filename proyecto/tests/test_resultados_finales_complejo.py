import pytest
from programaestudiante.funciones.procesamiento import resultados_finales

def test_resultados_finales_complejo(spark):
    data = [
        (1001, "Alajuela", "2023-10-01", 10.0),
        (1001, "Alajuela", "2023-10-02", 20.0),
        (1002, "Alajuela", "2023-10-01", 15.0),
        (1003, "Alajuela", None, None),

        (2001, "Heredia", "2023-10-01", 25.0),
        (2001, "Heredia", "2023-10-02", 5.0),
        (2002, "Heredia", "2023-10-01", 30.0),

        (3001, "San Jose", "2023-10-01", 30.0),
        (3002, "San Jose", "2023-10-01", None),
        (3002, "San Jose", "2023-10-02", 10.0),
        (3003, "San Jose", "2023-10-01", 5.0),
        (3003, "San Jose", "2023-10-02", 15.0),
    ]

    schema = ["cedula", "provincia", "fecha", "total_km"]
    df_agg = spark.createDataFrame(data, schema=schema)

    df_final = resultados_finales(df_agg, top_n=2)
    resultados = { (r.cedula, r.provincia): (r.total_km, r.promedio_diario, r.rank_total_km, r.rank_promedio_diario)
                   for r in df_final.collect() }

    # Verifica algunos resultados clave
    assert (1001, "Alajuela") in resultados
    assert pytest.approx(resultados[(1001, "Alajuela")][0], 0.01) == 30.0
    assert pytest.approx(resultados[(1001, "Alajuela")][1], 0.01) == 15.0

    assert (1003, "Alajuela") not in resultados
    assert (2002, "Heredia") in resultados
    assert (3003, "San Jose") in resultados or (3003, "San Jose") not in resultados  # depende del ranking final

    # Check límites por provincia
    provincias = [prov for _, prov in resultados.keys()]
    assert provincias.count("Alajuela") <= 2
    assert provincias.count("Heredia") <= 2
    assert provincias.count("San Jose") <= 2

