import dlt
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def declare_bronze_table(dlayer, reading_layer, table_name, storage_location):
  saving_path="mlibre."+dlayer + "."+table_name
  reading_location = storage_location+ "/"+reading_layer + "/"+ reading_layer + "/"+table_name
  if reading_layer == "source":
    reading_location = storage_location+ "/"+reading_layer + "/"+table_name

  @dlt.table(
    name=dlayer+"_"+table_name,
    comment="The raw"+ table_name+"ingested from sources container.",
    table_properties={
      "MlibrePipeline.quality": dlayer,
      "pipelines.autoOptimize.managed": "true"
    }
  )
  def table_bronze():
    return  spark.read.option("inferSchema", "false").option("header", "true").csv(reading_location)
  print("Table "+ table_name+" created in the "+dlayer+" layer, from "+reading_layer+" and saved in "+saving_path)

def declare_quarantine_table(dlayer, reading_layer, table_name, storage_location, quarantine_rules):
  saving_path="mlibre."+dlayer + "."+table_name
  @dlt.table(
    name=dlayer+"_"+table_name,
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