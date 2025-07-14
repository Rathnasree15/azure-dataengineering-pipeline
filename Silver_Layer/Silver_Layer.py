# Databricks notebook source
from pyspark.sql.functions import*
from pyspark.sql.types import*

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data

# COMMAND ----------

df_cal = spark.read.format('csv')\
.option("header", True)\
.option("inferSchema", True)\
.load("abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cus = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_procat = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_pro = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_ret = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sales = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_ter = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_subcat = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@awstoragedl15.dfs.core.windows.net/AdventureWorks_Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calendar

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date')))\
    .withColumn('Year', year(col('Date')))

# COMMAND ----------

df_cal.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Calendar").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn('FullName', concat_ws(' ','Prefix','FirstName','LastName'))


# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Customers")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub Categories

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
    .mode('append').option("path","abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split('ProductSKU','-')[0])\
    .withColumn('ProductName',split('ProductName',' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Products")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
    .mode('append')\
        .option("path", "abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Returns")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Territories").save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace('OrderNumber', 'S', 'T') )

# COMMAND ----------

df_sales = df_sales.withColumn('multiply', col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedl15.dfs.core.windows.net/AdventureWorks_Sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

