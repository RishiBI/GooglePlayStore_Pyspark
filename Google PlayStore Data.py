# Databricks notebook source
/FileStore/tables/apps.csv

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.load('/FileStore/tables/apps.csv',format='csv', sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df.count()

# COMMAND ----------

df.show(1)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.drop("Size","Content_Rating","Last_Updated","Android_Ver")

# COMMAND ----------

df.show(2)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,col
df=df.withColumn("Reviews",col("Reviews").cast(IntegerType()))\
.withColumn("Installs",regexp_replace(col("Installs"),"[^0-9]",""))\
.withColumn("Installs",col("Installs").cast(IntegerType()))\
.withColumn("Price",regexp_replace(col("Price"),"[$]",""))\
.withColumn("Price",col("Price").cast(IntegerType()))

# COMMAND ----------

df.createOrReplaceTempView("apps")

# COMMAND ----------

# MAGIC %sql select app,sum(reviews) from apps group by 1 order by 2 desc

# COMMAND ----------

# MAGIC %sql select app,sum(installs) from apps group by 1 order by 2 desc

# COMMAND ----------

# MAGIC %sql select category,sum(reviews) from apps group by 1 order by 2 desc

# COMMAND ----------

# MAGIC %sql select app,sum(price) from apps where type='Paid' group by 1 order by 2 desc
