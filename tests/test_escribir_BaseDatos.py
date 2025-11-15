
from pyspark.sql import SparkSession
from pyspark.sql import Row
from functools import reduce
from funciones.procesamiento import escribir_BaseDatos
from unittest.mock import MagicMock
from pyspark.sql import SparkSession, Row

import pytest
from unittest.mock import MagicMock
from pyspark.sql import Row
from pyspark.sql.readwriter import DataFrameWriter

def test_escribir_BaseDatos(spark, monkeypatch):
    # 1. A simple DataFrame
    df_final = spark.createDataFrame([Row(a=1), Row(a=2)])

    # 2. Patch ONLY the save() method of DataFrameWriter
    mock_save = MagicMock(name="save")
    monkeypatch.setattr(DataFrameWriter, "save", mock_save)

    # 4. Run function (will hit DataFrameWriter.save)
    escribir_BaseDatos(spark, df_final)

    # 5. Assert save() was called once
    mock_save.assert_called_once()
