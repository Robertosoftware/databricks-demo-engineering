# Databricks notebook source
dbutils.fs.ls('/Volumes/mlibre/ecommerce/quarantine')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

df = spark.read.format('delta').load('/Volumes/mlibre/ecommerce/quarantine/quarantine/buyers/')

# COMMAND ----------

display(df)

# COMMAND ----------


