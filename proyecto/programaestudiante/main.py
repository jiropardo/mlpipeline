import sys
from pyspark.sql import SparkSession
from funciones.procesamiento import leer_datos, limpiar_datos, join_datos

def main():
    if len(sys.argv) != 4:
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

    spark = SparkSession.builder.appName("Programa Estudiante").getOrCreate()

    # Leer datos
    # df_AsaltosUltimoAnioraw, df_Ingresopromedioestimadoraw, df_Educacionedadraw, df_IBMraw, df_Educacionsexoraw, df_ICraw, df_Esperanzadevidaraw, df_IDHraw = leer_datos(spark, path_AsaltosUltimoAnio,path_Ingresopromedioestimado,path_Educacionedad,path_IBM,path_Educacionsexo,path_IC,path_Esperanzadevida,path_IDH)
    
    df_AsaltosUltimoAnioraw = leer_datos(spark, path_AsaltosUltimoAnio,path_Ingresopromedioestimado,path_Educacionedad,path_IBM,path_Educacionsexo,path_IC,path_Esperanzadevida,path_IDH)
    
    df_AsaltosUltimoAnioraw.show(5)
    
    # Limpiar 
  
    
    
    # Unir datos


    spark.stop()

if __name__ == "__main__":
    main()

