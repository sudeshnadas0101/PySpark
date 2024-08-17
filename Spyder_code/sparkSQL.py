# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 01:30:50 2024

@author: dassu
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc


spark = SparkSession.builder.appName("sparkSQL").getOrCreate()
#spark.stop()
spark.sparkContext.setLogLevel("ERROR")
print(spark.version)

#this is a DataFrameReader
data = spark.read.format('csv')\
            .option("inferschema","true")\
            .option("header","true")\
            .option("path", "operations_management.csv")\
            .load()

data_2 = data.select("industry", "value").\
    filter((col("value") > 200) & (col("industry") != "total")).\
    orderBy(desc("value"))
    
data_2.show(5)

data_2.createOrReplaceTempView("data")

spark.sql("""Select industry, value
          from data
          where industry <> "total" and
          value > 200
          """).show()
#spark.sql("select * from data")

#data_2.createOrReplaceGlobalTempView("test1")

#data_3 = spark.sql("""
                   #SELECT * from test1
                   #""")


#spark.catalog.listDatabases()

#spark.catalog.listTables()

data_2.write.format('csv')\
    .option("mode","overwrite")\
    .option("path","E:\MyPrograms\PySpark\PySpark\Spyder_code\op")\
    .partitionBy("age")\
    .save()
            


