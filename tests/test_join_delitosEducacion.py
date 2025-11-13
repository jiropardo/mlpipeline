import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from funciones.procesamiento import join_delitosEducacion


def test_join_delitosEducacion(spark):
    # DataFrame de delitos
    df_delitos = spark.createDataFrame([
        Row(Provincia="Pichincha", Canton="Quito", ASALTO_ARMADO="SI",
            Victima="Adulto", Edad_Victima="30", Sexo_Victima="M", Total_Asaltos=5),
        Row(Provincia="Guayas", Canton="Guayaquil", ASALTO_ARMADO="NO",
            Victima="Adulto", Edad_Victima="25", Sexo_Victima="F", Total_Asaltos=3)
    ])

    # DataFrame de educación
    df_educacion = spark.createDataFrame([
        Row(Provincia="Pichincha", Canton="Quito", Nivel_Educativo="Primaria",
            Sexo="M", Total_Personas_Canton=100),
        Row(Provincia="Guayas", Canton="Guayaquil", Nivel_Educativo="Secundaria",
            Sexo="F", Total_Personas_Canton=80)
    ])

    # Ejecutar la función
    df_result = join_delitosEducacion(df_delitos, df_educacion)

    # Verificar columnas
    expected_columns = [
        "Provincia", "Canton", "ASALTO_ARMADO", "Victima", "Edad_Victima",
        "Sexo_Victima", "Total_Asaltos", "Nivel_Educativo", "Sexo", "Total_Personas_Canton"
    ]
    assert df_result.columns == expected_columns

    # Verificar número de filas
    assert df_result.count() == 2

    # Verificar join correcto
    result_dict = {(row.Provincia, row.Canton): (row.ASALTO_ARMADO, row.Nivel_Educativo) for row in df_result.collect()}
    assert result_dict[("Pichincha","Quito")] == ("SI", "Primaria")
    assert result_dict[("Guayas","Guayaquil")] == ("NO", "Secundaria")