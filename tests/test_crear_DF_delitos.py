from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DataType, DateType
from pyspark.sql.functions import col, when
from funciones.procesamiento import crear_DF_Delitos

def test_crear_DF_Delitos():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

    spark = SparkSession.builder.master("local[1]").appName("pytest_crear_DF_Delitos").getOrCreate()

    data = [
        ("ROBO", "ROBO CON ARMA", None, "Persona", "Adulto", 1, "25", "M", "Ecuatoriano", "Pichincha", "Quito"),
        ("ROBO", "ROBO SIN ARMA", None, "Persona", "Adulto", 2, "30", "F", "Ecuatoriano", "Pichincha", "Quito"),
        ("ROBO", "ROBO CON ARMA", None, "Persona", "Adulto", 3, "45", "M", "Ecuatoriano", "Guayas", "Guayaquil"),
    ]

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

    df_input = spark.createDataFrame(data, schema=schema)

    result_df = crear_DF_Delitos(spark, df=df_input)

    # Verificaciones simples
    result_data = [row.asDict() for row in result_df.collect()]
    expected_cols = ["ASALTO_ARMADO", "Provincia", "Canton", "Victima", "Edad_Victima", "Sexo_Victima", "Total_Asaltos"]
    assert all(c in result_df.columns for c in expected_cols)
    assert len([r for r in result_data if r["ASALTO_ARMADO"] == "SI"]) == 2
    assert len([r for r in result_data if r["ASALTO_ARMADO"] == "NO"]) == 1

    spark.stop()
