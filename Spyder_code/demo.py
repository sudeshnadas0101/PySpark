# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 19:35:38 2024

@author: dassu
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

data = [
        ('Sudeshna', '', 'Das', '1998-03-24', 'F', 100000),
        ('Kunwar', 'G', 'Pratap', '1800-03-18', 'M', 10)]

columns = ["first_name", "middle_name", "last_name", "DOB", "Gender", "Salary"]

df = spark.createDataFrame(data = data, schema = columns)

print(df.printSchema())
