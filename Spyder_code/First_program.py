# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 21:14:58 2024

@author: dassu
"""



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as func

#creating a Spark Session
spark = SparkSession.builder.appName("FirstApp").getOrCreate()
#spark.stop()
spark.sparkContext.setLogLevel("ERROR")


#creating a schema for Dataframe
myschema = StructType([
                StructField("user_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("friends", IntegerType(), True)])
                 
#creating dataframe from a csv file
people = spark.read.format("csv").schema(myschema)\
            .option("path", "fakefriends.csv")\
            .load()
            
#Transformation of data
output = people.select(people.user_id, people.name,\
                       people.age, people.friends)\
                .where(people.age < 30)\
                .withColumn('timestamp_data', func.current_timestamp())\
                .orderBy(people.user_id)
                
#creating temporary view
output.createOrReplaceTempView("people_s")
temp_views = spark.catalog.listTables()

# Check if 'people_s' is in the list of tables and views
if any(tbl.name == "people_s" and tbl.isTemporary for tbl in temp_views):
    print("Temporary view 'people_s' exists.")
else:
    print("Temporary view 'people_s' does not exist.")

#running a simple spark SQL query
spark.sql("select * from people_s").show()

output.write.format("json")\
    .mode("overwrite")\
    .option("path", "file:///E:/MyPrograms/PySpark/PySpark/Spyder_code/op_json/")\
    .partitionBy("age") \
    .save()

            
    
            