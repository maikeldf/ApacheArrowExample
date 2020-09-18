import pandas as pd
import numpy as np
import pyspark
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PyArrow_Test").enableHiveSupport().getOrCreate()

# Creating two different pandas DataFrame with same data
pdf1 = pd.DataFrame(np.random.rand(100000, 3))
pdf2 = pd.DataFrame(np.random.rand(100000, 3))

start = time.time()
spark.createDataFrame(pdf1)
end = time.time()
print("\nConversion time of Pandas DataFrames to Spark DataFrames: {:.2f}s\n".format(end - start))

# Using PyArrow in Spark to optimize the above Conversion

# Enable Arrow-based columnar data transfers in Spark
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

start = time.time()
df2 = spark.createDataFrame(pdf2)
end = time.time()
print("\nConversion time of Pandas DataFrames to Spark DataFrames using PyArrow: {:.2f}s\n".format(end - start))
