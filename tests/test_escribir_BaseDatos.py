
from pyspark.sql import SparkSession
from pyspark.sql import Row
from functools import reduce
from funciones.procesamiento import escribir_BaseDatos
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, Row

def test_escribir_BaseDatos(spark, monkeypatch):

     # 2. Create a simple DataFrame
    df_final = spark.createDataFrame([Row(a=1), Row(a=2)])

    # 3. Take the real Spark writer
    real_writer = df_final.write

    # 4. Intercept ONLY the final save() so Spark never loads JDBC driver
    real_writer.save = MagicMock(name="save")

    # 5. Replace df.write with our modified writer
    monkeypatch.setattr(df_final, "write", real_writer)

    # 6. Run your function (will NOT attempt to load the driver)
    escribir_BaseDatos(spark, df_final)

    # 7. Assert that save() was called exactly once
    real_writer.save.assert_called_once()