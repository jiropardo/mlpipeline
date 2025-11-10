import findspark
import os
import sys
from pyspark.sql import SparkSession
from funciones.procesamiento import crear_DF_Delitos, crear_DF_Educacion ,  crear_DF_Ingreso, crear_DF_idh, crear_DF_ev, crear_DF_ibm, crear_DF_idc, join_delitosEducacion, join_todo

from pyspark.sql.types import (StringType, IntegerType, FloatType, 
                               DecimalType, StructField, StructType, DoubleType)


jar_path = "/src/sparkml/postgresql-42.2.14.jar"

# Ensure file exists
if not os.path.exists(jar_path):
    raise FileNotFoundError(f"PostgreSQL JAR not found: {jar_path}")

# Export to environment so JVM definitely sees it
os.environ["SPARK_CLASSPATH"] = jar_path


def main():
    if len(sys.argv) != 9:
        print("Uso: main.py AsaltosUltimoAnio.csv Ingresopromedioestimado.csv Educacionedad.csv IBM.csv Educacionsexo.csv IC.csv Esperanzadevida.csv IDH.csv")
        sys.exit(1)

    path_AsaltosUltimoAnio = sys.argv[1]
    path_Ingresopromedioestimado = sys.argv[2]
    path_Educacionedad = sys.argv[3]
    path_IBM = sys.argv[4]
    path_Educacionsexo = sys.argv[5]
    path_IC = sys.argv[6]
    path_Esperanzadevida = sys.argv[7]
    path_IDH = sys.argv[8]
    
    # spark = SparkSession \
    # .builder \
    # .appName("Programa Estudiante") \
    # .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
    # .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
    # .getOrCreate()
    
    spark = SparkSession.builder \
    .appName("Programa Estudiante") \
    .config("spark.driver.extraClassPath", jar_path) \
    .config("spark.executor.extraClassPath", jar_path) \
    .getOrCreate()

    #Creacion de DataFrames
    
    # DF_Delitos
    df_delitos = crear_DF_Delitos(spark, path_AsaltosUltimoAnio)

    #df_delitos.show(5)
    
    # DF_Educacion
    df_educacion = crear_DF_Educacion(spark, path_Educacionedad, path_Educacionsexo)
    #df_educacion.show(5)
    
    ## DF_Ingreso
    df_ingreso = crear_DF_Ingreso(spark, path_Ingresopromedioestimado)
    #df_ingreso.show(5)
    
    ## DF_idh
    df_idh = crear_DF_idh(spark, path_IDH)
    #df_idh.show(5)
    
    ## DF_ev
    df_ev = crear_DF_ev(spark, path_Esperanzadevida)
   # df_ev.show(5)
    
    ## DF_ibm
    df_ibm = crear_DF_ibm(spark, path_IBM)   
    #df_ibm.show(5)
    
    ## DF_idc
    df_idc = crear_DF_idc(spark, path_IC)
    #df_idc.show(5)
    
    # Join Datos
    
    ## Join Delitos y Educacion
    df_delitosEducacion = join_delitosEducacion(df_delitos, df_educacion)
    
    # df_delitosEducacion.show(5)
    
    ## join_delitosEducacion con indices
    
    df_final = join_todo(df_delitosEducacion, df_ingreso, df_idh, df_ev, df_ibm, df_idc) 
    
    df_final.show(5)
    print(spark.sparkContext.getConf().get("spark.jars"))
    
    # escribir Datos
    df_final.write \
    .format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:postgresql://172.17.0.1:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", "DatosUnidos") \
    .save()
  
    spark.stop()

if __name__ == "__main__":
    main()

