# Databricks notebook source
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "4645191d-8348-48e3-bdb8-2a83cc9c5d0d",
    "fs.azure.account.oauth2.client.secret":"abY8Q~tI5d4B96E4d5MWhsiymdmErGXc-DuubcIU",
    "fs.azure.account.oauth2.client.endpoint":"https://login.microsoftonline.com/87f050b0-4ec1-410d-81d7-97b2c1e92b04/oauth2/v2.0/token"
}



# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalakehousegen23/bronze/SalesLT"))

# COMMAND ----------

df =spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/Address/Address.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

selected_col_df = df.select("AddressID","AddressLine1","City","StateProvince","CountryRegion","ModifiedDate")
display(selected_col_df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

Date_modify_df = selected_col_df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd")) 
display(Date_modify_df)

# COMMAND ----------

column_renamed_df = Date_modify_df.withColumnRenamed("AddressLine1", "AddressLine") \
    .withColumnRenamed("StateProvince", "State")\
    .withColumnRenamed("CountryRegion", "Country") \
    
display(column_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import col
order_df = column_renamed_df.orderBy(col("Country").asc(), col("State").asc(), col("City").asc(), col("ModifiedDate").asc())
display(order_df)


# COMMAND ----------

order_df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/Address")

# COMMAND ----------

# MAGIC %md
# MAGIC Data Transformation-Customer table 

# COMMAND ----------


df =spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/Customer/Customer.parquet")
display(df)

# COMMAND ----------

customer_selected_df = df.select("CustomerID", "FirstName", "LastName",  "EmailAddress", "CompanyName", "ModifiedDate")
display(customer_selected_df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

Customer_Date_modify_df = customer_selected_df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd")) 
display(Customer_Date_modify_df)

# COMMAND ----------

from pyspark.sql.functions import col
Customer_orderby_df = Customer_Date_modify_df.orderBy(col("FirstName").asc(), col("ModifiedDate").asc())
display(Customer_orderby_df)

# COMMAND ----------

Customer_orderby_df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/Customer")

# COMMAND ----------

# MAGIC %md
# MAGIC DATA TRANSFORMATION -CustomerAddress

# COMMAND ----------

df =spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/CustomerAddress/CustomerAddress.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

CustomerAddress_Date_modify_df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd")) 
display(CustomerAddress_Date_modify_df)

# COMMAND ----------

from pyspark.sql.functions import col
customeraddress_selected_df = CustomerAddress_Date_modify_df.select("CustomerID","AddressID","AddressType","ModifiedDate")\
    .orderBy(col("AddressType").asc(),col("ModifiedDate").asc())
display(customeraddress_selected_df)

# COMMAND ----------

customeraddress_selected_df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/CustomerAddress")

# COMMAND ----------

# MAGIC %md
# MAGIC data transformation-Product 

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/Product/Product.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

product_data_modify_df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd")) 
display(product_data_modify_df)

# COMMAND ----------

from pyspark.sql.functions import col
product_selected_df = product_data_modify_df.select("ProductID", "ProductCategoryID","ProductModelID","Name", "ProductNumber", "Color", "StandardCost", "Size","ModifiedDate")\
    .withColumnRenamed("Name","ProductName") \
    .orderBy(col("Size").asc())
display(product_selected_df)

# COMMAND ----------

# Create a temporary view from the DataFrame
product_selected_df.createOrReplaceTempView("product_selected_view")

query = """
SELECT 
    ProductID,ProductCategoryID,ProductModelID,ProductName, ProductNumber, Color, StandardCost, Size,ModifiedDate,
    CASE
        WHEN size >= 38 AND size <= 40 THEN 'M'
        WHEN size > 40 AND size <= 44 THEN 'L'
        WHEN size > 44 AND size <= 70 THEN 'XL'
        WHEN size > 70 AND size <= 75 THEN 'XXL'
    END AS size_category
FROM product_selected_view
"""

modified_product_df = spark.sql(query)
display(modified_product_df)

# COMMAND ----------

new_product_df= modified_product_df.select("ProductID", "ProductCategoryID","ProductModelID","ProductName", "ProductNumber", "Color", "StandardCost", "Size", "size_category", "ModifiedDate")
display(new_product_df)

# COMMAND ----------

dropnull_product_df = new_product_df.dropna(subset=['Color', 'Size'])
display(dropnull_product_df)

# COMMAND ----------

from pyspark.sql.functions import col
correct_product_df = dropnull_product_df.orderBy(col("Color").asc())\
    .dropna(subset=['size_category'])\
    

display(correct_product_df)

# COMMAND ----------

from pyspark.sql.functions import concat, lit, format_number

# Assuming you have a DataFrame `df` with a 'StandardCost' column
# First, format the 'StandardCost' to two decimal places
df_formatted = correct_product_df.withColumn("StandardCost", format_number(df["StandardCost"], 3))

# Now, concatenate the Euro symbol with the formatted 'StandardCost'
df_with_euro = df_formatted.withColumn("StandardCost", concat(lit("€"), df_formatted["StandardCost"]))
display(df_with_euro)


# COMMAND ----------

df_with_euro.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/Product")

# COMMAND ----------

# MAGIC %md
# MAGIC data transformation-productcategory 
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/ProductCategory/ProductCategory.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col
productcategory_df = df.withColumnRenamed("Name","ProductName")\
    .select("ProductCategoryID","ParentProductCategoryID","ProductName","ModifiedDate")\
    .orderBy(col("ProductName").asc())\
    .dropna(subset=['ParentProductCategoryID'])\
    
display(productcategory_df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

newproductcategory_df = productcategory_df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd")) 
display(newproductcategory_df)

# COMMAND ----------

newproductcategory_df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/ProductCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC data transformation- ProductModel 

# COMMAND ----------

df =spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/ProductModel/ProductModel.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format,col
from pyspark.sql.types import TimestampType

pm_df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))\
    .withColumnRenamed("Name","ProductModelName")\
    .select("ProductModelID","ProductModelName","ModifiedDate")\
    .orderBy(col("ModifiedDate").asc())
    
display(pm_df)

# COMMAND ----------

pm_df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/ProductModel")

# COMMAND ----------

# MAGIC %md
# MAGIC Data transformation-SalesOrderDetails

# COMMAND ----------

df =spark.read.option("header",True).option("inferSchema",True).parquet("/mnt/datalakehousegen23/bronze/SalesLT/SalesOrderDetail/SalesOrderDetail.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format,col
from pyspark.sql.types import TimestampType

SOD_df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))\
    .select("SalesOrderID","SalesOrderDetailID","ProductID","OrderQty","ModifiedDate")\
    .orderBy(col("OrderQty").asc())
    
display(SOD_df)

# COMMAND ----------

SOD_df.write.mode("overwrite").parquet("/mnt/datalakehousegen23/silver/SalesLT/SalesOrderDetails")
