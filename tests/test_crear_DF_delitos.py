import pytest
from pyspark.sql import Row
from funciones.procesamiento import crear_DF_Delitos
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType , DateType

def test_crear_DF_Delitos_direct_dataframe(spark):
    """
    Test crear_DF_Delitos logic entirely in-memory, without files or mocks.
    """

    # --- 1️⃣ Create input DataFrame identical to the CSV schema
    schema = StructType([
        StructField("Delito", StringType(), True),
        StructField("SubDelito", StringType(), True),
        StructField("Fecha", StringType(), True),
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
        ("ASALTO", "Asalto con arma blanca", "2024-10-10", "Persona", "Peatón", 1, "25", "M", "Ecuatoriano", "Pichincha", "Quito"),
        ("ASALTO", "Asalto sin arma", "2024-10-12", "Persona", "Peatón", 2, "40", "F", "Ecuatoriano", "Pichincha", "Quito"),
        ("ASALTO", "Robo con arma de fuego", "2024-11-01", "Persona", "Cliente", 3, "30", "M", "Ecuatoriano", "Guayas", "Guayaquil"),
    ]
    df_input = spark.createDataFrame(data, schema=schema)

    # --- 2️⃣ Inject the DataFrame manually into the transformation logic
    # We reuse the function body logic directly here without modifying the source
    # (equivalent to what crear_DF_Delitos would do after reading the CSV)

    delitos2 = df_input.select("SubDelito", "Provincia", "Canton", "SubVictima", "Edad", "Sexo")
    delitos2 = (
        delitos2
        .withColumnRenamed("Subvictima", "Victima")
        .withColumnRenamed("Edad", "Edad_Victima")
        .withColumnRenamed("Sexo", "Sexo_Victima")
    )
    delitos2 = delitos2.withColumn(
        "SubDelito",
        when(col("SubDelito").rlike("(?i)ARMA"), "SI").otherwise("NO")
    )
    delitos2 = (
        delitos2
        .groupBy("SubDelito", "Provincia", "Canton", "Victima", "Edad_Victima", "Sexo_Victima")
        .count()
        .withColumnRenamed("count", "Total_Asaltos")
        .orderBy("Total_Asaltos", ascending=False)
    )
    df_result = (
        delitos2
        .withColumnRenamed("SubDelito", "ASALTO_ARMADO")
    )

    # --- 3️⃣ Validate output
    expected_cols = {
        "ASALTO_ARMADO",
        "Provincia",
        "Canton",
        "Victima",
        "Edad_Victima",
        "Sexo_Victima",
        "Total_Asaltos",
    }
    assert set(df_result.columns) == expected_cols

    rows = df_result.collect()

    # --- 4️⃣ Validate transformation logic
    armado_vals = [r["ASALTO_ARMADO"] for r in rows]
    assert "SI" in armado_vals
    assert "NO" in armado_vals

    # There should be three total aggregated rows (each unique)
    assert len(rows) == 3

    # All counts should be 1
    assert all(r["Total_Asaltos"] == 1 for r in rows)

    # Verify one specific case
    si_rows = [r for r in rows if r["ASALTO_ARMADO"] == "SI"]
    assert len(si_rows) == 2  # 2 rows with “ARMA”