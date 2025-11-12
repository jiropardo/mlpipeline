from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when

def test_crear_DF_Delitos_direct_dataframe(spark):
    """
    Test de la función crear_DF_Delitos usando DataFrames creados directamente en memoria.
    """

    # --- 1️⃣ Crear DataFrame de entrada que imite el CSV original
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

    # --- 2️⃣ Datos de ejemplo (uno con “ARMA” → “SI”, y otro sin → “NO”)
    data = [
        ("ASALTO", "Asalto con arma blanca", "2024-10-10", "Persona", "Peatón", 1, "25", "M", "Ecuatoriano", "Pichincha", "Quito"),
        ("ASALTO", "Asalto sin arma", "2024-10-12", "Persona", "Peatón", 2, "40", "F", "Ecuatoriano", "Pichincha", "Quito"),
        ("ASALTO", "Robo con arma de fuego", "2024-11-01", "Persona", "Cliente", 3, "30", "M", "Ecuatoriano", "Guayas", "Guayaquil"),
        ("ASALTO", "Hurto simple", "2024-11-02", "Persona", "Cliente", 4, "22", "F", "Ecuatoriano", "Guayas", "Daule"),  # este debe dar "NO"
    ]
    df_input = spark.createDataFrame(data, schema=schema)

    # --- 3️⃣ Aplicar la misma lógica que en crear_DF_Delitos
    delitos2 = df_input.select("SubDelito", "Provincia", "Canton", "SubVictima", "Edad", "Sexo")
    delitos2 = (
        delitos2
        .withColumnRenamed("SubVictima", "Victima")
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

    df_result = delitos2.withColumnRenamed("SubDelito", "ASALTO_ARMADO")

    # --- 4️⃣ Validaciones
    expected_cols = {
        "ASALTO_ARMADO",
        "Provincia",
        "Canton",
        "Victima",
        "Edad_Victima",
        "Sexo_Victima",
        "Total_Asaltos",
    }
    assert set(df_result.columns) == expected_cols, "❌ Las columnas no coinciden con las esperadas."

    rows = df_result.collect()

    # --- 5️⃣ Verificar que haya tanto 'SI' como 'NO'
    armado_vals = [r["ASALTO_ARMADO"] for r in rows]
    assert "SI" in armado_vals, "❌ No se encontró ningún caso con ASALTO_ARMADO = SI"
    assert "NO" in armado_vals, "❌ No se encontró ningún caso con ASALTO_ARMADO = NO"

    print("✅ Test crear_DF_Delitos_direct_dataframe pasó correctamente.")
