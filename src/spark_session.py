from pyspark.sql import SparkSession
import setting

spark = SparkSession.builder.master(setting.spark_master).appName("nyc_data").getOrCreate()
