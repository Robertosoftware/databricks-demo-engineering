-- Databricks notebook source
GRANT SELECT ON SHARE mlibre TO RECIPIENT inteliia;


-- COMMAND ----------

USE CATALOG mlibre

-- COMMAND ----------

USE SCHEMA dev_ecommerce

-- COMMAND ----------

ALTER SHARE mlibre ADD SCHEMA dev_ecommerce COMMENT "Hello World"

-- COMMAND ----------

ALTER SHARE mlibre ADD TABLE bronze_transactions COMMENT "Hello World"

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS

-- COMMAND ----------

DESCRIBE STORAGE CREDENTIAL ddevmlibre1dbw;

-- COMMAND ----------

show storage credentials

-- COMMAND ----------

DESCRIBE STORAGE CREDENTIAL ddevtbauc1dbw;

-- COMMAND ----------



-- COMMAND ----------

DROP STORAGE CREDENTIAL IF EXISTS ddevtbauc1dbw FORCE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC check_locations = []
-- MAGIC
-- MAGIC query = "SHOW EXTERNAL LOCATIONS"
-- MAGIC result = spark.sql(query)
-- MAGIC for row in result.collect():
-- MAGIC   check_locations.append(row["url"])
-- MAGIC
-- MAGIC #######################
-- MAGIC #      catalogs       #
-- MAGIC #######################
-- MAGIC
-- MAGIC query = "SHOW CATALOGS"
-- MAGIC result = spark.sql(query)
-- MAGIC for row in result.collect():
-- MAGIC   catalog_name = row["catalog"]
-- MAGIC   query = f"DESCRIBE CATALOG EXTENDED {catalog_name}"
-- MAGIC   result = spark.sql(query)
-- MAGIC   df = pd.DataFrame(result.collect()).transpose()
-- MAGIC   if not df.empty:
-- MAGIC     df.columns = df.iloc[0]
-- MAGIC     df = df[1:]
-- MAGIC     if 'Storage Location' in df.columns:
-- MAGIC       location = df.iloc[0]["Storage Location"]
-- MAGIC       for check_location in check_locations:
-- MAGIC         if check_location in location:
-- MAGIC           print(catalog_name)
-- MAGIC           print(f"\t{location}")
-- MAGIC
-- MAGIC #######################
-- MAGIC #       schemas       #
-- MAGIC #######################
-- MAGIC
-- MAGIC   query = f"SHOW SCHEMAS IN {catalog_name}"
-- MAGIC   result = spark.sql(query)
-- MAGIC   for row in result.collect():
-- MAGIC     schema_name = row["databaseName"]
-- MAGIC     query = f"DESCRIBE SCHEMA {catalog_name}.{schema_name}"
-- MAGIC     result = spark.sql(query)
-- MAGIC     df = pd.DataFrame(result.collect()).transpose()
-- MAGIC     if not df.empty:
-- MAGIC       df.columns = df.iloc[0]
-- MAGIC       df = df[1:]
-- MAGIC       if 'Location' in df.columns:
-- MAGIC         location = df.iloc[0]["Location"]
-- MAGIC         for check_location in check_locations:
-- MAGIC           if check_location in location:
-- MAGIC             print(catalog_name)
-- MAGIC             print(f'\t{schema_name}')
-- MAGIC             print(f"\t\t{location}")
-- MAGIC         
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC     if schema_name != 'information_schema':      
-- MAGIC       query = f"SHOW TABLES IN {catalog_name}.{schema_name}"
-- MAGIC       result = spark.sql(query)
-- MAGIC       for row in result.collect():
-- MAGIC         table_name = row["tableName"]
-- MAGIC         query = f"DESCRIBE detail  {catalog_name}.{schema_name}.{table_name}"
-- MAGIC         result = spark.sql(query)
-- MAGIC         for row in spark.sql(query).collect():
-- MAGIC           print
-- MAGIC           location = row["location"]
-- MAGIC           for check_location in check_locations:
-- MAGIC             if check_location in location:
-- MAGIC               print(catalog_name)
-- MAGIC               print(f'\t{schema_name}')
-- MAGIC               print(f'\t\t{table_name}')
-- MAGIC               print(f"\t\t\t{location}")
