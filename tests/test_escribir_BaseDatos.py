
from pyspark.sql import SparkSession
from pyspark.sql import Row
from functools import reduce
from funciones.procesamiento import escribir_BaseDatos

def test_escribir_BaseDatos_real(spark):

    # ---- 2. Create simple DataFrame in memory ----
    input_data = [Row(a=1), Row(a=2), Row(a=3)]
    df_final = spark.createDataFrame(input_data)

    # ---- 3. Run your function (writes to PostgreSQL) ----
 # <--- change module name
    escribir_BaseDatos(spark, df_final)

    # ---- 4. Read the table back from PostgreSQL ----
    df_read = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.17.0.1:5433/postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "postgres") \
        .option("password", "testPassword") \
        .option("dbtable", "DatosUnidos") \
        .load()

    # ---- 5. Assertions ----
    # row count must match
    assert df_read.count() == len(input_data)

    # check values
    read_values = [row["a"] for row in df_read.collect()]
    assert set(read_values) == {1, 2, 3}
