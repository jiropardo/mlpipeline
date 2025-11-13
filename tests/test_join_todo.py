import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from functools import reduce
from funciones.procesamiento import join_todo


def test_join_todo(spark):
    # DataFrame de delitosEducation
    df_delitosEducation = spark.createDataFrame([
        Row(Canton="Quito", Provincia="Pichincha", ASALTO_ARMADO="SI", Victima="Adulto",
            Edad_Victima="30", Sexo_Victima="M", Total_Asaltos=5, Nivel_Educativo="Primaria",
            Sexo="M", Total_Personas_Canton=100),
        Row(Canton="Guayaquil", Provincia="Guayas", ASALTO_ARMADO="NO", Victima="Adulto",
            Edad_Victima="25", Sexo_Victima="F", Total_Asaltos=3, Nivel_Educativo="Secundaria",
            Sexo="F", Total_Personas_Canton=80)
    ])

    # DataFrames adicionales
    df_idh = spark.createDataFrame([
        Row(Canton="Quito", IndiceDesarolloHumano="0.80"),
        Row(Canton="Guayaquil", IndiceDesarolloHumano="0.90")
    ])

    df_ingreso = spark.createDataFrame([
        Row(Canton="Quito", IngresoPromedio="2000"),
        Row(Canton="Guayaquil", IngresoPromedio="3000")
    ])

    df_ev = spark.createDataFrame([
        Row(Canton="Quito", EsperanzaVida="78"),
        Row(Canton="Guayaquil", EsperanzaVida="81")
    ])

    df_idc = spark.createDataFrame([
        Row(Canton="Quito", IndiceConocimiento="80"),
        Row(Canton="Guayaquil", IndiceConocimiento="90")
    ])

    df_ibm = spark.createDataFrame([
        Row(Canton="Quito", IndiceBienestarMaterial="60"),
        Row(Canton="Guayaquil", IndiceBienestarMaterial="70")
    ])

    # Ejecutar la función
    df_result = join_todo(df_delitosEducation, df_idh, df_ingreso, df_ev, df_idc, df_ibm)

    # Verificar columnas clave
    expected_columns = [
        "Canton", "Provincia", "ASALTO_ARMADO", "Victima", "Edad_Victima", "Sexo_Victima",
        "Total_Asaltos", "Nivel_Educativo", "Sexo", "Total_Personas_Canton",
        "IndiceDesarolloHumano", "IngresoPromedio", "EsperanzaVida",
        "IndiceConocimiento", "IndiceBienestarMaterial"
    ]
    assert sorted(df_result.columns) == sorted(expected_columns)

    # Verificar número de filas
    assert df_result.count() == 2

    # Verificar join correcto
    result_dict = {row.Canton: row.Total_Asaltos for row in df_result.collect()}
    assert result_dict["Quito"] == 5
    assert result_dict["Guayaquil"] == 3
