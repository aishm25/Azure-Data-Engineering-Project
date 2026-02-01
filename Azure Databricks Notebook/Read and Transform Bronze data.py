# Databricks notebook source
# MAGIC %md
# MAGIC # **SILVER LAYER SCRIPT**

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA ACCESS USING APP

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC **Read Calendar Data**

# COMMAND ----------

df_cal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Calendar")


# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading Bronze data

# COMMAND ----------

df_cus= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Customers")


# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_prod_cat= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Product_Categories")


# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prod= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_returns= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_sales= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Sales_*")

# COMMAND ----------

df_sales.sort("OrderDate").show()


# COMMAND ----------

df_ter= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_ter.show()

# COMMAND ----------

df_prod_sub_cat= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@storageazureprojectaish.dfs.core.windows.net/Product_Subcategories/AdventureWorks_Product_Subcategories.csv")

# COMMAND ----------

df_prod_sub_cat.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Calendar**
# MAGIC *Adding 2 new columns in Calendar df*

# COMMAND ----------

from pyspark.sql.functions import month, year

df_cal = df_cal.withColumn("Month", month("Date")).withColumn("Year", year("Date"))
display(df_cal)

# COMMAND ----------

# MAGIC %md
# MAGIC Lets push this df to Silver layer in Parquet format

# COMMAND ----------

df_cal.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Calendar") \
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC **Customers**

# COMMAND ----------

# MAGIC %md
# MAGIC Creating FullName column by concatenating 3 columns

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col

df_cus = df_cus.withColumn("FullName", concat_ws(" ", col("prefix"), col("FirstName"), col("LastName")))
display(df_cus)


# COMMAND ----------

df_cus.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Customers") \
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Sub Categories 

# COMMAND ----------

df_prod_sub_cat.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Product_Subcategories") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

df_prod.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Lets create a new column having just the first 2 letters from the ProductSKU column

# COMMAND ----------

from pyspark.sql.functions import substring_index

df_prod = df_prod.withColumn("SKU_Prefix", substring_index("ProductSKU", "-", 1))
display(df_prod)

# COMMAND ----------

df_prod.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Products") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Returns") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Territories") \
    .save()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prod_cat.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Product_Categories") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Data

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Add year / month / day columns

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth

df_sales = (
    df_sales
    .withColumn("OrderYear", year("OrderDate"))
    .withColumn("OrderMonth", month("OrderDate"))
    .withColumn("OrderDay", dayofmonth("OrderDate"))
)
df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets change datatype for a column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

df_sales = df_sales.withColumn("Stockdate", to_timestamp("Stockdate"))
display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Line-level quantity flag

# COMMAND ----------

from pyspark.sql.functions import when

df_sales = df_sales.withColumn(
    "IsBulkOrder",
    when(df_sales.OrderQuantity >= 2, 1).otherwise(0)
)
df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total quantity per order

# COMMAND ----------

from pyspark.sql.functions import sum

df_order_summary = (
    df_sales
    .groupBy("OrderNumber")
    .agg(sum("OrderQuantity").alias("TotalOrderQty"))
)
df_order_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace 

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_sales = df_sales.withColumn("OrderNumber",regexp_replace(col("OrderNumber"),'S','T'))
df_sales.display()

# COMMAND ----------

df_sales.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Sales") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Analysis

# COMMAND ----------

df_prod.write.format("parquet") \
    .mode("append") \
    .option("path", "abfss://silver@storageazureprojectaish.dfs.core.windows.net/AdventureWorks_Products") \
    .save()from pyspark.sql.functions import sum,desc

df_order_byday = (
    df_sales
    .groupBy("OrderDate")
    .agg(sum("OrderQuantity").alias("TotalOrderByDay"))
    .orderBy(desc("TotalOrderByDay"))
)

df_order_byday.display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_prod.describe()

# COMMAND ----------

