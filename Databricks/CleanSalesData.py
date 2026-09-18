# Databricks notebook source
# DBTITLE 1,Connecting with Azure
path = "abfss://raw-data@crmsalesdataset.dfs.core.windows.net/"
    
display(dbutils.fs.ls(path))

# COMMAND ----------

# DBTITLE 1,dataframe of all data tables
df_accounts = spark.read.csv("abfss://raw-data@crmsalesdataset.dfs.core.windows.net/accounts.csv",header=True)
df_dataDict = spark.read.csv("abfss://raw-data@crmsalesdataset.dfs.core.windows.net/data_dictionary.csv",header=True)
df_products = spark.read.csv("abfss://raw-data@crmsalesdataset.dfs.core.windows.net/products.csv",header=True)
df_sales = spark.read.csv("abfss://raw-data@crmsalesdataset.dfs.core.windows.net/sales_pipeline.csv",header=True)
df_salesTeam = spark.read.csv("abfss://raw-data@crmsalesdataset.dfs.core.windows.net/sales_teams.csv",header=True)


# COMMAND ----------

display(df_accounts)
display(df_dataDict)
# display(df_products)
display(df_sales)
# display(df_salesTeam)

# COMMAND ----------

# DBTITLE 1,Transform Accounts Table
from pyspark.sql.functions import col, sum

df_accounts = df_accounts.select(
    col("account").cast("string").alias('company_Name'),
    col("sector").alias('industry'),
    col("year_established").cast("int").alias('stablished_year'),
    col("revenue").cast("decimal(10,2)").alias('Annual_revenue'),
    col("employees").cast("int").alias("employee_Count"),
    col("office_location").cast("string").alias("officeLocation"),
    col("subsidiary_of").alias("parent_company")
    )

#Check if table column contains null values
df_accounts.select(sum(col("parent_company").isNull().cast("int"))).show()
#fill null values with a defined value for now
df_accounts_clean = df_accounts.fillna("Unknown", subset=["parent_company"])

display(df_accounts_clean)

# COMMAND ----------

# DBTITLE 1,Product Table Transformation
df_products = df_products.select(
    col("product").cast("string").alias('product'),
    col("series").cast("string").alias('series'),
    col("sales_price").cast("decimal(10,2)")
    )

# Check for Nulls
df_products.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_products.columns
]).show()  

df_products_clean = df_products

# COMMAND ----------

# DBTITLE 1,Transform Sales Data

df_sales = df_sales.select(
    col("opportunity_id").cast("string").alias('sales_ID'),
    col("sales_agent").alias('sales_Agent'),
    col("product").alias('product_Name'),
    col("account").alias('company_name'),
    col("deal_stage").cast("string").alias('deal_Stage'),
    col("engage_date").cast("date").alias('engage_Date'),
    col("close_date").cast("date").alias('close_Date'),
    col("close_value").cast("decimal(10,2)").alias("Deal_Revenue")
    )

df_sales.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_sales.columns
]).show()  

df_sales_clean = df_sales.fillna({
    "company_name": "Unknown"
})

display(df_sales_clean)



# COMMAND ----------

df_salesTeam = df_salesTeam.select(
    col("sales_agent").cast("string").alias('sales_agent'),
    col("manager").cast("string").alias('manager'),
    col("regional_office").cast("string").alias("regional_Office")
)

df_salesTeam.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_salesTeam.columns
]).show()  

df_salesTeam_clean = df_salesTeam


# COMMAND ----------

display(df_accounts_clean)
display(df_products_clean)
display(df_sales_clean)
display(df_salesTeam_clean)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS accounts")
df_accounts_clean.write.mode("overwrite").option("path", "abfss://transformed-data@crmsalesdataset.dfs.core.windows.net/accounts").saveAsTable("accounts")


# COMMAND ----------

df_products_clean.write.mode("overwrite").option("path", "abfss://transformed-data@crmsalesdataset.dfs.core.windows.net/products").saveAsTable("products")
df_sales_clean.write.mode("overwrite").option("path", "abfss://transformed-data@crmsalesdataset.dfs.core.windows.net/sales").saveAsTable("sales")
df_salesTeam_clean.write.mode("overwrite").option("path", "abfss://transformed-data@crmsalesdataset.dfs.core.windows.net/sales_teaam").saveAsTable("sales_team")

# COMMAND ----------

