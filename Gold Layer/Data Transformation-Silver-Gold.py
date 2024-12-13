# Databricks notebook source
# MAGIC %md
# MAGIC Rename the Column Header  \
# MAGIC Convert paraquet to delta file format 

# COMMAND ----------

dbutils.fs.ls('/mnt/datalakehousegen23/silver/SalesLT')

# COMMAND ----------

table_name=[]

for i in dbutils.fs.ls('/mnt/datalakehousegen23/silver/SalesLT'):
    table_name.append(i.name.split('/')[0])
table_name

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

for name in table_name:
    df =spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/silver/SalesLT/" + name)

    #get the list of column names
    column_names = df.columns 

    for old_col_name in column_names:
        #conver to column name from ColumnName to Column_Name format
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
        #change the column name using withColumnRenamed and regexp_replace
        df =  df.withColumnRenamed(old_col_name, new_col_name)
    
    df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/gold/SalesLT/" + name)

# COMMAND ----------

dbutils.fs.ls('/mnt/datalakehousegen23/gold/SalesLT')

# COMMAND ----------

display(df)

# COMMAND ----------

table_name=[]

for i in dbutils.fs.ls('/mnt/datalakehousegen23/gold/SalesLT'):
    table_name.append(i.name.split('/')[0])
table_name

# COMMAND ----------

for name in table_name:
    parquet_table_paths = [
        f.path for f in dbutils.fs.ls("/mnt/datalakehousegen23/gold/SalesLT/" + name)
        if f.name.endswith(".parquet")
    ]
    
    delta_dir = "/mnt/datalakehousegen23/gold/SalesLTDelta/" + name
    if dbutils.fs.mkdirs(delta_dir):
        delta_table_paths = [
            f.path for f in dbutils.fs.ls(delta_dir)
            if f.name.endswith(".delta")
        ]
    else:
        delta_table_paths = []

for parquet_table_paths, delta_table_paths in zip(parquet_table_paths, delta_table_paths):
    # Read the Parquet table into a DataFrame
    parquet_df = spark.read.parquet(parquet_table_paths)
    
    # Write the DataFrame in Delta format to the specified location
    parquet_df.write.format("delta").mode("overwrite").save(delta_table_paths)
    
    print(f"Converted {parquet_table_paths} to Delta format at {delta_table_paths}")

# COMMAND ----------

display(dbutils.fs.ls('/mnt/datalakehousegen23/gold/SalesLTDelta'))
