from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, when
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType , DateType
from pyspark.sql.functions import col, upper, translate
from pyspark.sql.functions import sum
from functools import reduce
from pyspark.sql import DataFrame


### DF delitos

def crear_DF_Delitos(spark, path_AsaltosUltimoAnio=None, df=None):
    """
    Lee los CSV sin header y limpia los datso de delitos
    """
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
    
    df_AsaltosUltimoAnioraw = spark.read \
    .option("header", True) \
    .option("dateFormat", "yyyy-MM-dd")\
    .schema(schema) \
    .csv(path_AsaltosUltimoAnio)
    
    delitos2 = df_AsaltosUltimoAnioraw .select("SubDelito", "Provincia", "Canton", "Subvictima", "Edad", "Sexo")

    # Renombras la columna y reasignas
    delitos2 = (
        delitos2
        .withColumnRenamed("Subvictima", "Victima")
        .withColumnRenamed("Edad", "Edad_Victima")
        .withColumnRenamed("Sexo", "Sexo_Victima")
    )

    #remplazar por arma

    delitos2 = delitos2.withColumn(
        "SubDelito",
        when(col("SubDelito").rlike("(?i)ARMA"), "SI").otherwise("NO")
    )

    #aggrupas

    delitos2 = (
        delitos2
        .groupBy("SubDelito", "Provincia", "Canton", "Victima", "Edad_Victima", "Sexo_Victima")
        .count()
        .withColumnRenamed("count", "Total_Asaltos")
        .orderBy("Total_Asaltos", ascending=False)
    )

    df_delitos = (
        delitos2
        .withColumnRenamed("SubDelito", "ASALTO_ARMADO")
        .withColumnRenamed("Edad", "Edad_Victima")
        .withColumnRenamed("Sexo", "Sexo_Victima")
    )

    return  df_delitos

#### DF_Educacion

def crear_DF_Educacion(spark, path_Educacionedad, path_Educacionsexo):
    pass

### educacion por edad
    # Step 2: schema
    schema = StructType([
        StructField("Nivel_Educativo", StringType(), True),
        StructField("Rango_Edad", StringType(), True),
        StructField("Canton", StringType(), True),
        StructField("Distrito", StringType(), True),
        StructField("Provincia", StringType(), True),
        StructField("IsGrandTotalRowTotal", BooleanType(), True),
        StructField("Numero_Personas", IntegerType(), True)
    ])

    # Step 4: Read CSV with schema
    df_educacion_por_edad = spark.read \
        .option("header", True) \
        .option("delimiter", ";") \
        .schema(schema) \
        .csv(path_Educacionedad)

    #Selccionando columnas 

    df_educacion_por_edad = df_educacion_por_edad.dropna()

    df_educacion_por_edad = df_educacion_por_edad.select(["Nivel_Educativo", "Rango_Edad", "Canton", "Distrito", "Provincia"])
    
    ### Educacion por sexo 
    
    #Educacion por sexo DATAFRAME

    schema = StructType([
        StructField("Nivel_Educativo", StringType(), True),
        StructField("Sexo", StringType(), True),
        StructField("Canton", StringType(), True),
        StructField("Distrito", StringType(), True),
        StructField("Provincia", StringType(), True),
        StructField("IsGrandTotalRowTotal", BooleanType(), True),
        StructField("DistinctCountsnb003_id_persona", IntegerType(), True)
    ])

    # Leer el archivo CSV con el esquema definido
    df_educacion_por_sexo = spark.read.option("header", True).option("delimiter", ";").schema(schema).csv(path_Educacionsexo)

   
        # Join usando la columna "Nivel Educativo" 
    df_joined = df_educacion_por_sexo.join(
        df_educacion_por_edad,
        on=["Nivel_Educativo", "Canton", "Distrito", "Provincia"]
    )
    
    df_joined_2 = df_joined.withColumnRenamed("DistinctCountsnb003_id_persona", "Cantidad_Personas_Distrito") \
                         .withColumnRenamed("Numero_Personas", "Total")
                         
        # Filter out unknown education levels
    df_educacion = df_joined_2.where("Nivel_Educativo != 'DESCONOCIDO'")

    # Rename the column and save back to the DataFrame
    df_educacion = df_educacion.withColumnRenamed("Rango_Edad", "Rango_Edad_Educacion")
    
    # Encontrar total personas por canton
    df_educacion = df_educacion.groupBy("Nivel_Educativo", "Canton", "Provincia", "Sexo").agg(
    sum("Cantidad_Personas_Distrito").alias("Total_Personas_Canton")
    )

    return df_educacion
                 
#### DF_Ingreso
    
def crear_DF_Ingreso(spark, path_Ingresopromedioestimado):
    # Define schema with all fields as StringType
    schema = StructType([
        StructField("Canton", StringType(), True),
        StructField("2010", StringType(), True),
        StructField("2011", StringType(), True),
        StructField("2012", StringType(), True),
        StructField("2013", StringType(), True),
        StructField("2014", StringType(), True),
        StructField("2015", StringType(), True),
        StructField("2016", StringType(), True),
        StructField("2017", StringType(), True),
        StructField("2018", StringType(), True),
        StructField("2019", StringType(), True),
        StructField("2020", StringType(), True),
    ])

    # Read CSV as-is (semicolon separated, all as string)
    df_ingreso = spark.read.csv(path_Ingresopromedioestimado, header=True, sep=";", schema=schema).dropna(subset=["Canton"]).withColumnRenamed("2020", "IngresoPromedio")

    df_ingreso = df_ingreso.select("Canton", "IngresoPromedio")

    df_ingreso = df_ingreso.withColumn(
        "Canton_clean",
        upper(
            translate(
                col("Canton"),
                "áéíóúÁÉÍÓÚ",
                "aeiouAEIOU"
            )
        )
    )

    df_ingreso = df_ingreso.select("Canton_clean", "IngresoPromedio").withColumnRenamed("Canton_clean", "Canton")
    
    return df_ingreso
    
#### DF_idh
    
def crear_DF_idh(spark, path_idh):

    schema = StructType([
        StructField("Canton", StringType(), True),
        StructField("2010", StringType(), True),
        StructField("2011", StringType(), True),
        StructField("2012", StringType(), True),
        StructField("2013", StringType(), True),
        StructField("2014", StringType(), True),
        StructField("2015", StringType(), True),
        StructField("2016", StringType(), True),
        StructField("2017", StringType(), True),
        StructField("2018", StringType(), True),
        StructField("2019", StringType(), True),
        StructField("2020", StringType(), True),
    ])


    # Read CSV as-is (semicolon separated, all as string)
    df_idh = spark.read.csv(path_idh, header=True, sep=";", schema=schema).dropna(subset=["Canton"]).withColumnRenamed("2020", "IndiceDesarolloHumano")

    df_idh = df_idh.select("Canton", "IndiceDesarolloHumano")


    df_idh = df_idh.withColumn(
        "Canton_clean",
        upper(
            translate(
                col("Canton"),
                "áéíóúÁÉÍÓÚ",
                "aeiouAEIOU"
            )
        )
    )

    df_idh = df_idh.select("Canton_clean", "IndiceDesarolloHumano").withColumnRenamed("Canton_clean", "Canton")
    
    return df_idh
    
#### DF_ev
    
def crear_DF_ev(spark, path_Esperanzadevida):
    schema = StructType([
        StructField("Canton", StringType(), True),
        StructField("2010", StringType(), True),
        StructField("2011", StringType(), True),
        StructField("2012", StringType(), True),
        StructField("2013", StringType(), True),
        StructField("2014", StringType(), True),
        StructField("2015", StringType(), True),
        StructField("2016", StringType(), True),
        StructField("2017", StringType(), True),
        StructField("2018", StringType(), True),
        StructField("2019", StringType(), True),
        StructField("2020", StringType(), True),
    ])

    df_ev = spark.read.csv(path_Esperanzadevida, header=True, sep=";", schema=schema).dropna(subset=["Canton"]).withColumnRenamed("2020", "EsperanzaVida")

    df_ev = df_ev.select("Canton", "EsperanzaVida")

    df_ev = df_ev.withColumn(
        "Canton_clean",
        upper(
            translate(
                col("Canton"),
                "áéíóúÁÉÍÓÚ",
                "aeiouAEIOU"
            )
        )
    )

    df_ev = df_ev.select("Canton_clean", "EsperanzaVida").withColumnRenamed("Canton_clean", "Canton")
    
    return df_ev
    
#### DF_ibm
def crear_DF_ibm(spark, path_IBM):
    schema = StructType([
    StructField("Canton", StringType(), True),
    StructField("2010", StringType(), True),
    StructField("2011", StringType(), True),
    StructField("2012", StringType(), True),
    StructField("2013", StringType(), True),
    StructField("2014", StringType(), True),
    StructField("2015", StringType(), True),
    StructField("2016", StringType(), True),
    StructField("2017", StringType(), True),
    StructField("2018", StringType(), True),
    StructField("2019", StringType(), True),
    StructField("2020", StringType(), True),
])

    df_ibm = spark.read.csv(path_IBM, header=True, sep=";", schema=schema).dropna(subset=["Canton"]).withColumnRenamed("2020", "IndiceBienestarMaterial")

    df_ibm = df_ibm.select("Canton", "IndiceBienestarMaterial")

    df_ibm = df_ibm.withColumn(
        "Canton_clean",
        upper(
            translate(
                col("Canton"),
                "áéíóúÁÉÍÓÚ",
                "aeiouAEIOU"
            )
        )
    )

    df_ibm = df_ibm.select("Canton_clean", "IndiceBienestarMaterial").withColumnRenamed("Canton_clean", "Canton")
    
    return df_ibm   
#### DF_idc
    
def crear_DF_idc(spark, path_IC):
    # Define schema with all fields as StringType
    schema = StructType([
        StructField("Canton", StringType(), True),
        StructField("2010", StringType(), True),
        StructField("2011", StringType(), True),
        StructField("2012", StringType(), True),
        StructField("2013", StringType(), True),
        StructField("2014", StringType(), True),
        StructField("2015", StringType(), True),
        StructField("2016", StringType(), True),
        StructField("2017", StringType(), True),
        StructField("2018", StringType(), True),
        StructField("2019", StringType(), True),
        StructField("2020", StringType(), True),
    ])

    df_ic = spark.read.csv(path_IC, header=True, sep=";", schema=schema).dropna(subset=["Canton"]).withColumnRenamed("2020", "IndiceConocimiento")

    df_ic = df_ic.select("Canton", "IndiceConocimiento")

    df_ic = df_ic.withColumn(
        "Canton_clean",
        upper(
            translate(
                col("Canton"),
                "áéíóúÁÉÍÓÚ",
                "aeiouAEIOU"
            )
        )
    )

    df_ic = df_ic.select("Canton_clean", "IndiceConocimiento").withColumnRenamed("Canton_clean", "Canton")
    
    return df_ic
    

def join_delitosEducacion(df_delitos, df_educacion):
    df_delitosEducation = df_delitos.join(df_educacion, on=["Provincia", "Canton"], how="inner") 

    # Select only the columns you need and order by Total_Asaltos
    df_delitosEducation = (
        df_delitosEducation
        .select(
            "Provincia",
            "Canton",
            "ASALTO_ARMADO",       # Make sure this column exists
            "Victima",
            "Edad_Victima",
            "Sexo_Victima",
            "Total_Asaltos",
            "Nivel_Educativo",
            "Sexo",
            "Total_Personas_Canton"
        )
        .orderBy(col("Total_Asaltos").desc())  # alternative syntax for descending
    )
    
    return df_delitosEducation

def join_todo(df_delitosEducation, df_idh, df_ingreso, df_ev, df_idc, df_ibm):
    
    dfs_to_join = [df_delitosEducation, df_idh, df_ingreso, df_ev, df_idc, df_ibm]

# Perform successive joins on 'Canton'
    df_merged = reduce(lambda left, right: left.join(right, on="Canton", how="inner"), dfs_to_join)

    # Show result
    df_merged = df_merged.dropna()

    df_merged.orderBy("Total_Asaltos", ascending=False)
    
    return df_merged

def escribir_BaseDatos(spark, df_final):
    
    df_final.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.17.0.1:5433/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", "DatosUnidos") \
    .mode("overwrite") \
    .save()




