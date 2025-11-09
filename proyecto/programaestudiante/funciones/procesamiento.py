from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, when
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def leer_datos(spark, path_AsaltosUltimoAnio, path_Ingresopromedioestimado, path_Educacionedad, path_IBM, path_Educacionsexo, path_IC, path_Esperanzadevida, path_IDHs):
    """
    Lee los CSV sin header y asigna nombres correctos a las columnas.
    """
    
    schema = StructType([
    StructField("Nivel_Educativo", StringType(), True),
    StructField("Rango_Edad", StringType(), True),
    StructField("Canton", StringType(), True),
    StructField("Distrito", StringType(), True),
    StructField("Provincia", StringType(), True),
    StructField("IsGrandTotalRowTotal", BooleanType(), True),
    StructField("Numero_Personas", IntegerType(), True)
    ])
    
    df_AsaltosUltimoAnioraw = spark.read \
    .option("header", True) \
    .option("delimiter", ";") \
    .schema(schema) \
    .csv(path_AsaltosUltimoAnio)
    
    df_AsaltosUltimoAnioraw= df_AsaltosUltimoAnioraw.dropna()

    df_AsaltosUltimoAnioraw = df_AsaltosUltimoAnioraw.select(["Nivel_Educativo", "Rango_Edad", "Canton", "Distrito", "Provincia"])
    

    return df_AsaltosUltimoAnioraw
    #return ciclistas_df, rutas_df, actividades_df


# df_AsaltosUltimoAnioraw 
# df_Ingresopromedioestimadoraw
# df_Educacionedadraw, df_IBMraw
# df_Educacionsexoraw
# df_ICraw
# df_Esperanzadevidaraw
# df_IDHra


