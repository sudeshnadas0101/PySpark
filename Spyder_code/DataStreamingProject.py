# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 19:23:29 2024

@author: dassu
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder.appName("Data_Streaming_Project").getOrCreate()
spark.sparkContext.setLogLevel("INFO")


#Defining Input sources
lines = spark.readStream.format("socket").option("host", "localhost")\
    .option("port", 9999).load()
    
#Transform Data
words = lines.select(split(col("value"), "\\s").alias("word"))

#Get count of published words
counts = words.groupBy("word").count()

#Defining the checkpoint directory
checkpointDir = "C:/checkpoint/"

#Start streaming defining the necessary configurations
streamingQuery = (counts.writeStream\
                  .format("console").outputMode("complete")\
                  .trigger(processingTime = "1 second")\
                  .option("checkpointLocation", checkpointDir)\
                  .start())
streamingQuery.awaitTermination()