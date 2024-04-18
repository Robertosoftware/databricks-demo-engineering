import dlt
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def declare_bronze_table(dlayer, reading_layer, table_name, storage_location):
  saving_path= storage_location+ dlayer+"01/"+dlayer + "/"+table_name
  @dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The raw"+ table_name+"ingested from sources container.",
    table_properties={
      "MlibrePipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
  def table_bronze():
    return  spark.read.option("inferSchema", "false").option("header", "true").csv(storage_location+reading_layer+"01/"+table_name+"/")
  print("Table "+ table_name+" created in the "+dlayer+" layer, from "+reading_layer+" and saved in "+saving_path)

def declare_quarantine_table(dlayer, reading_layer, table_name, storage_location, quarantine_rules):
  saving_path= storage_location+ dlayer+"01/"+dlayer + "/"+table_name
  @dlt.table(
    name=dlayer+"_"+table_name,
    path=saving_path,
    comment="The raw quarantined data from"+ table_name,
    table_properties={
      "MlibrePipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
  @dlt.expect_or_drop("valid_quarantine",quarantine_rules)
  def buyers_quarantine():
      return dlt.read(reading_layer+"_"+table_name)
  print("Table "+ table_name+" created in the "+dlayer+" layer, from "+reading_layer+" and saved in "+saving_path)