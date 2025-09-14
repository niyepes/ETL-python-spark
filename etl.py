#Crear Sesión

from pyspark.sql import SparkSession
from pyspark import SparkContext

SpSession = SparkSession \
          .builder \
          .appName("Demo Spark") \
          .getOrCreate()
     

SpContext = SpSession.sparkContext
     
#Cargar Datos

data = SpSession.read.csv('cars.csv', header=True, sep=";")
data.show(5)

#Para conocer los datos
data.printSchema()

print(data.columns)
print(data.dtypes)

#Formas de seleccionar columnas

from os import truncate
#MÉTODO 1
data.select(data.Car).show(truncate=False)


#MÉTODO 2
data.select(data['car']).show(truncate=False)

#MÉTODO 3
from pyspark.sql.functions import col
data.select(col('car')).show(truncate=False)


#Selección multiple de columnas

#Forma 1
data.select(data.Car, data.Cylinders).show(truncate=False)

#Forma 2
data.select(data['car'], data['cylinders']).show(truncate=False)

#Forma 3
from pyspark.sql.functions import col
data.select(col('CAR'), col('CYLINDERS')).show(truncate=False)

#Agregación de nuevas columnas

#CASO 1
from pyspark.sql.functions import lit

df = data.withColumn('First_Column',lit(1))
df.show(5, truncate=False)

#CASO 2
df = data.withColumn('Second_Column',lit(2)) \
          .withColumn('third_column',lit('Third Column'))
df.show(5, truncate=False)

#Agrupación

print(df.groupBy('Horsepower').count().show(5))

print(df.groupBy('Origin','Model').count().show(10))

#Eliminar Columnas

df = df.drop('Second_Column','third_column')
print(df.show(5))

total_count = df.count()
print("Total de registros:", total_count)

europa= df.filter(col('Origin')=="US").count()
print("Total de registros en US:", europa)

df.filter(col('Origin')=="US").show(truncate=False)

total_count = df.count()
print("Total de registros:", total_count)
usa= df.filter((col('Origin')=="US")&(col('Horsepower')=="175.0")).count()
print("Total de registros:", usa)

df.filter((col('Origin')=="US")&(col('Horsepower')=="175.0")).show(truncate=False)

#Ordenar Filas
df.orderBy('Cylinders').show(truncate=False)

df.orderBy('Cylinders',ascending=False).show(truncate=False)

df.groupBy('Origin').count().orderBy('count', ascending=False).show(10)
     
