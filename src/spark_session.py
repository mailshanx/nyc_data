from pyspark.sql import SparkSession
import setting
import pyspark


def get_spark_session():
    config = pyspark.SparkConf().setAll([('spark.executor.memory', setting.spark_executor_memory),
                                         ("spark.app.name", setting.spark_app_name),
                                         ("spark.master", setting.spark_master),
                                         ('spark.driver.memory', setting.spark_driver_memory)])
    _spark = SparkSession.builder.config(conf=config).getOrCreate()
    _spark.sparkContext.setLogLevel(setting.spark_error_level)
    return _spark


spark = get_spark_session()
