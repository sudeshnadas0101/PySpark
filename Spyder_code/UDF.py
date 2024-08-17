# -*- coding: utf-8 -*-
"""
Created on Sat Aug 17 14:47:05 2024

@author: dassu
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import LongType
import pandas as pd

spark = SparkSession.builder.appName("udf_implement").getOrCreate()

#df = spark.range(1,5)


def cube_cal(a : pd.Series) -> pd.Series:
    return a * a * a

cubed_udf = pandas_udf(cube_cal, returnType=LongType())

x = pd.Series([1,2,3,4,5])

print(cube_cal(x))
