from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, DateType
from pyspark.sql.functions import col, when
from funciones.procesamiento import crear_DF_Delitos

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from funciones.procesamiento import crear_DF_Delitos  # Ajusta la importación según tu proyecto
from pyspark.sql import Row
import datetime


def test_crear_DF_Delitos_inmemory(spark):
    schema = StructType([
        StructField("Delito", StringType(), True),
        StructField("SubDelito", StringType(), True),
        StructField("Fecha", DateType(), True),
        StructField("Victima", StringType(), True),
        StructField("SubVictima", StringType(), True),
        StructField("x", IntegerType(), True),
        StructField("Edad", StringType(), True),
        StructField("Sexo", StringType(), True),
        StructField("Nacionalidad", StringType(), True),
        StructField("Provincia", StringType(), True),
        StructField("Canton", StringType(), True),
    ])

    data = [
        Row(
            Delito="Robo",
            SubDelito="Robo con ARMA de fuego",
            Fecha=datetime.date(2025, 1, 1),
            Victima="Persona",
            SubVictima="Adulto",
            x=1,
            Edad="30",
            Sexo="M",
            Nacionalidad="EC",
            Provincia="Pichincha",
            Canton="Quito"
        ),
        Row(
            Delito="Robo",
            SubDelito="Robo simple",
            Fecha=datetime.date(2025, 1, 2),
            Victima="Persona",
            SubVictima="Adulto",
            x=2,
            Edad="25",
            Sexo="F",
            Nacionalidad="EC",
            Provincia="Pichincha",
            Canton="Quito"
        ),
    ]

    df_input = spark.createDataFrame(data, schema)
    df_result = crear_DF_Delitos(spark, df=df_input)

    expected_columns = ["ASALTO_ARMADO", "Provincia", "Canton", "Victima", "Edad_Victima", "Sexo_Victima", "Total_Asaltos"]
    assert df_result.columns == expected_columns

    result_dict = {row.Victima + row.Edad_Victima + row.Sexo_Victima: row.ASALTO_ARMADO for row in df_result.collect()}
    assert result_dict["Adulto30M"] == "SI"
    assert result_dict["Adulto25F"] == "NO"

    counts = {row.ASALTO_ARMADO: row.Total_Asaltos for row in df_result.collect()}
    assert counts["SI"] == 1
    assert counts["NO"] == 1