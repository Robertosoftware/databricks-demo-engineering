# Databricks notebook source
# MAGIC %md
# MAGIC * Author: Roberto Bonilla
# MAGIC * Date: 10/03/2024
# MAGIC * Version: v2.0
# MAGIC * Comments: Assessment TB Auctions
# MAGIC
# MAGIC ## Importing Libraries and preparing DLT Pipeline

# COMMAND ----------

import dlt
from pyspark.sql.functions import lit, regexp_extract, to_timestamp, trim, expr
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating widget

# COMMAND ----------

dbutils.widgets.text('storage_account_location','/mnt/tf-abfssdev-')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Data Quality Checks

# COMMAND ----------

from libraries.dlt_pipeline.dlt_table_creation import ( declare_bronze_table, declare_quarantine_table )

# COMMAND ----------

from libraries.dlt_pipeline.quality_checks import ( rules_buyers, quarantine_rules_buyers, rules_auctions,rules_bids, quarantine_rules_bids )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Variables

# COMMAND ----------

sa_location = dbutils.widgets.get('storage_account_location')
dlayers = ['quarantine','source','bronze','silver','gold']
table_name = ['bids','auctions','buyers']

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer
# MAGIC
# MAGIC Approach: h: The bronze layer serves as the foundational stage in the data pipeline, capturing raw data in its most unaltered form. This layer emphasizes minimal modification, ensuring data authenticity and completeness by preserving original column names, data structures, and types. 

# COMMAND ----------

# DBTITLE 1,Bronze Auctions
declare_bronze_table(dlayers[2], dlayers[1], table_name[1], sa_location)

# COMMAND ----------

# DBTITLE 1,Bronze Bids
declare_bronze_table(dlayers[2], dlayers[1], table_name[0], sa_location)

# COMMAND ----------

# DBTITLE 1,Bronze Buyers
declare_bronze_table(dlayers[2], dlayers[1], table_name[2], sa_location)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Quarantine Layer
# MAGIC
# MAGIC Approach: The quarantine layer serves as a stage for data that requires validation, cleansing, or further examination due to potential quality issues or discrepancies. Data in this layer is temporarily isolated to prevent the propagation of errors or anomalies into the core data layers (silver & gold).

# COMMAND ----------

# DBTITLE 1,Quarantine Buyers
declare_quarantine_table(dlayers[0], dlayers[2], table_name[2], sa_location, quarantine_rules_buyers)

# COMMAND ----------

# DBTITLE 1,Quarantine Bids
declare_quarantine_table(dlayers[0], dlayers[2], table_name[0], sa_location, quarantine_rules_bids)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer
# MAGIC
# MAGIC Approach: Silver tables will involve moderate modifications from the bronze layer. This stage will focus on renaming columns, adjusting data types, and applying initial filters and transformations to refine the data. The purpose is to prepare a cleaner and more structured dataset for advanced analytics and reporting in the golden layer, while still preserving the essential characteristics and granularity of the original data.

# COMMAND ----------

# DBTITLE 1,Variables Layer
dlayer = dlayers[3]

# COMMAND ----------

# DBTITLE 1,Silver Buyers
table_name = "buyers"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
@dlt.expect_all_or_drop(rules_buyers)
def buyers_silver():
    return (
        dlt.read("bronze_buyers")
    )

# COMMAND ----------

# DBTITLE 1,Silver Auctions
table_name = "auctions"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
@dlt.expect_all_or_fail(rules_auctions)
def auctions_silver():
    df= dlt.read('bronze_auctions')
    df = df.select("auctionid", "auction_type").dropDuplicates()
    df = df.withColumn("auction_days", regexp_extract("auction_type", "([0-9]+)",1)) \
            .withColumn("auctionid", df["auctionid"].cast("bigint")) \
            .withColumnRenamed("auctionid","auction_id")
    df = df.withColumn("auction_days", df["auction_days"].cast("int"))
    return df

# COMMAND ----------

# DBTITLE 1,Silver Bids
table_name = "bids"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
@dlt.expect_all_or_drop(rules_bids)
def bids_silver():
    df = dlt.read('bronze_bids')
    df = df.selectExpr('auctionid as auction_id','bid','bidder', 'openbid as open_bid', 'itemid as item_id','item', 'item_description','price', 'datetime')
    df = df.withColumn("datetime", to_timestamp(df["datetime"], "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")) \
            .withColumn("auction_id", df["auction_id"].cast("bigint")) \
            .withColumn("open_bid", df["open_bid"].cast("double")) \
            .withColumn("bid", df["bid"].cast("double")) \
            .withColumn("price", df["price"].cast("double")) \
            .withColumn("item_description", trim(df["item_description"])) \
            .withColumn("item_id", df["item_id"].cast("bigint"))
    df = df.withColumn("date_bid", df["datetime"].cast("date")) 
    return df

# COMMAND ----------

# DBTITLE 1,Silver Auction_Sellers
table_name = "auction_sellers"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
def auction_sellers_silver():
    df = dlt.read('bronze_auctions')
    df = df.withColumn("seller_id", df["seller_id"].cast("int")) \
            .withColumn("auctionid", df["auctionid"].cast("bigint")) \
            .withColumnRenamed("auctionid","auction_id") \
            .select("seller_id", "auction_id").dropDuplicates()
    return df

# COMMAND ----------

# DBTITLE 1,Silver Sellers
table_name = "sellers"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
def sellers_silver():
    df = dlt.read('bronze_auctions')
    df = df.withColumn("seller_id", df["seller_id"].cast("int")) \
                .select("seller_id", "name","email", "username").dropDuplicates()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer
# MAGIC Approach: Gold tables are the culmination of the data engineering process, featuring highly curated and aggregated data ready for consumption by end-users. At this stage, significant transformations, data enrichments, and aggregations are applied to present the most relevant and insightful information. This layer focuses on creating a user-friendly dataset with optimized structures for reporting, analytics, and machine learning applications, ensuring high data quality and accessibility.

# COMMAND ----------

# DBTITLE 1,Variable Layer
dlayer = dlayers[4]
# COMMAND ----------

# DBTITLE 1,Gold Items
table_name = "items"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )

def items_gold():
    df= dlt.read('silver_bids').select("item_id", "item","item_description").drop_duplicates()
    return df

# COMMAND ----------

# DBTITLE 1,Gold Auctions
table_name = "auctions"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
def auctions_gold():
    return dlt.read('silver_auctions').select('auction_id','auction_type', 'auction_days')

# COMMAND ----------

# DBTITLE 1,Gold Bids
table_name = "bids"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
def bids_gold():
    df_bids =  dlt.read('silver_bids')
    df_auction_sellers =  dlt.read('silver_auction_sellers')
    df = df_bids.join(df_auction_sellers, 'auction_id', how="inner") \
            .select('bid','auction_id', 'item_id','seller_id','bidder','open_bid','price','datetime','date_bid')
    return df

# COMMAND ----------

# DBTITLE 1,Gold Buyers
table_name = "buyers"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
def buyers_gold():
    return dlt.read('silver_buyers')

# COMMAND ----------

# DBTITLE 1,Gold Sellers
table_name = "sellers"
saving_path= sa_location+ dlayer+"01/"+dlayer + "/"+table_name
@dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The "+dlayer+" for the "+ table_name,
    table_properties={
      "TbAucPipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
def sellers_gold():
    return dlt.read('silver_sellers')
