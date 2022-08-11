from pyspark.sql import *
from pyspark.sql.functions import *


#it creates the session
def started(appname , manager):
    spark = SparkSession.builder.appName(appname).master(manager).getOrCreate()
    return spark

#without defined schema
def without_schema(spark, format_, delimiter_, path):
    result = spark.read.format(format_).options(header=True,
     delimiter=delimiter_).load(path)
    return result

#gives the count of features
def count_len(file_name):
    return print(f"number of rows {file_name.count()} and columns {len(file_name.columns)}")

#it will read with the custom schema and return it
def createdf(spark, format_, schema_, delimiter_, path):
    result = spark.read.format(format_).options(header=True,
     delimiter=delimiter_).schema(schema_).load(path)
    return result

#print show and printschema
def printschema(file):
    return file.show(), file.printSchema() 

#it will write the file into parquet 
def write_to_csv(df, writemode, outputpath):
    result_wcsv = df.write.mode(writemode).parquet(outputpath)
    return result_wcsv

#it will create csv file
def create_to_csv(spark, format_, path_):
    result_csv = spark.read.format(format_).load(path_)
    return result_csv


def write_csv(file , format_, mode_, path_):
    write_csv = file.write.format(format_).mode(mode_).save(path_)
