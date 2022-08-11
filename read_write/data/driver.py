from pyspark import *
from utility import *
from pyspark.sql.types import *

data = r'C:\Users\sid\Downloads\code pil\DATA_CSV_MINE\read_write'

spark = started(appname="csv_app" , manager="local[2]")


without_schema_ = without_schema(spark, format_="csv", delimiter_=";", path=data+'/user.csv')

#print without schema 
printschema(without_schema_)


count_len(without_schema_)

schema= StructType([StructField('Username', StringType(), True),
                            StructField('Identifier', IntegerType(), True),
                            StructField('One-time password', IntegerType(), True),
                            StructField('Recovery code', StringType(),True),
                            StructField('First name', StringType(),True),
                            StructField('Last name', StringType(),True),
                            StructField('Department', StringType(),True),
                            StructField('Location', StringType(),True),
                            ])

#read file with given schema
user_df = createdf(spark, format_="csv", schema_=schema, delimiter_=';', path=data+'/user.csv')

#see the dataframe
printschema(user_df)

#count of features
count_len(user_df)

#write it in the append mode
written_df = write_to_csv(user_df, 'append', "Data_written_user_csv/user_df")


#it will return show,  printschema methods output
printschema(user_df)

#InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
online_schema = StructType([StructField("InvoiceNo", IntegerType(), True), 
                         StructField("StockCode", StringType(), True), 
                         StructField("Description", StringType(), True), 
                         StructField("Quantity", IntegerType(), True), 
                         StructField("InvoiceDate", TimestampType(), True),
                         StructField("UnitPrice", DoubleType(), True),
                         StructField("CustomerID", IntegerType(), True),
                         StructField("Country", StringType(), True), 
                        ])


#another data frame
df_online = createdf(spark, format_="csv", schema_=online_schema, delimiter_=',', path=data+'/OnlineRetail.csv')
 
printschema(df_online)


count_len(df_online)

#overwrite it into csv
online_written_csv = write_csv(df_online, "csv", 'overwrite', "Data_written_csv_overwritten/created_written_df")
